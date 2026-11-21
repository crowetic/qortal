package org.qortal.controller;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.qortal.controller.tradebot.TradeBot;
import org.qortal.crosschain.ACCT;
import org.qortal.crosschain.AcctMode;
import org.qortal.crosschain.SupportedBlockchain;
import org.qortal.data.at.ATData;
import org.qortal.data.block.BlockData;
import org.qortal.data.crosschain.CrossChainTradeData;
import org.qortal.event.Event;
import org.qortal.event.EventBus;
import org.qortal.event.Listener;
import org.qortal.repository.DataException;
import org.qortal.repository.Repository;
import org.qortal.repository.RepositoryManager;
import org.qortal.utils.ByteArray;
import org.qortal.utils.NamedThreadFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Derived, non-consensus index of current cross-chain offers.
 *
 * The repository remains authoritative. A complete snapshot is built once at startup and after an
 * orphan, while normal blocks only refresh ACCT ATs whose current-state head changed. Snapshot
 * replacement is atomic, so readers never observe a partially rebuilt view.
 */
public final class CrossChainOfferIndex implements Listener {

	private static final Logger LOGGER = LogManager.getLogger(CrossChainOfferIndex.class);
	private static final long PROCESS_INTERVAL_SECONDS = 5L;
	private static final long AUDIT_INTERVAL_MINUTES = 60L;
	private static final int SNAPSHOT_HISTORY_LIMIT = 64;
	private static final int MAX_INCREMENTAL_HEIGHTS = 256;

	public static final class Offer {
		private final CrossChainTradeData tradeData;
		private final byte[] creatorPublicKey;

		private Offer(CrossChainTradeData tradeData, byte[] creatorPublicKey) {
			this.tradeData = tradeData;
			this.creatorPublicKey = creatorPublicKey.clone();
		}

		public CrossChainTradeData getTradeData() {
			return this.tradeData;
		}

		public byte[] getCreatorPublicKey() {
			return this.creatorPublicKey.clone();
		}
	}

	public static final class Snapshot {
		private static final Snapshot EMPTY = new Snapshot(Collections.emptyMap(), 0, null, false);

		private final Map<String, Offer> offersByAT;
		private final Map<String, List<CrossChainTradeData>> offersByCreator;
		private final int height;
		private final byte[] signature;
		private final boolean ready;

		private Snapshot(Map<String, Offer> offersByAT, int height, byte[] signature, boolean ready) {
			this.offersByAT = Collections.unmodifiableMap(new HashMap<>(offersByAT));
			Map<String, List<CrossChainTradeData>> grouped = this.offersByAT.values().stream()
					.map(Offer::getTradeData)
					.collect(Collectors.groupingBy(data -> data.qortalCreator));
			Map<String, List<CrossChainTradeData>> immutableGrouped = new HashMap<>();
			grouped.forEach((creator, offers) -> immutableGrouped.put(creator,
					Collections.unmodifiableList(new ArrayList<>(offers))));
			this.offersByCreator = Collections.unmodifiableMap(immutableGrouped);
			this.height = height;
			this.signature = signature == null ? null : signature.clone();
			this.ready = ready;
		}

		public boolean isReady() {
			return this.ready;
		}

		public int getHeight() {
			return this.height;
		}

		public Map<String, Offer> getOffersByAT() {
			return this.offersByAT;
		}

		public Map<String, List<CrossChainTradeData>> getOffersByCreator() {
			return this.offersByCreator;
		}

		public byte[] getSignature() {
			return this.signature == null ? null : this.signature.clone();
		}
	}

	private static final class SingletonContainer {
		private static final CrossChainOfferIndex INSTANCE = new CrossChainOfferIndex();
	}

	private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
			new NamedThreadFactory("Cross-chain offer index", Thread.NORM_PRIORITY));
	private final AtomicBoolean started = new AtomicBoolean(false);
	private final Object pendingLock = new Object();
	/** Immutable AT definitions, learned once and reused by every block refresh. */
	private final Map<String, ATData> acctDefinitions = new HashMap<>();
	/** Addresses whose immutable code hash is not a supported ACCT. */
	private final Set<String> nonAcctAddresses = new HashSet<>();
	private final Map<ByteArray, Snapshot> snapshotHistory = new LinkedHashMap<ByteArray, Snapshot>() {
		@Override
		protected boolean removeEldestEntry(Map.Entry<ByteArray, Snapshot> eldest) {
			return size() > SNAPSHOT_HISTORY_LIMIT;
		}
	};
	private volatile Snapshot snapshot = Snapshot.EMPTY;
	private boolean fullRebuildPending = true;
	private Integer minimumPendingHeight;
	private byte[] pendingRestoreSignature;

	private CrossChainOfferIndex() {
		EventBus.INSTANCE.addListener(this);
	}

	public static CrossChainOfferIndex getInstance() {
		return SingletonContainer.INSTANCE;
	}

	public void start() {
		if (!this.started.compareAndSet(false, true))
			return;

		this.executor.scheduleWithFixedDelay(this::processPendingSafely, 1L,
				PROCESS_INTERVAL_SECONDS, TimeUnit.SECONDS);
		this.executor.scheduleAtFixedRate(this::requestFullRebuild, AUDIT_INTERVAL_MINUTES,
				AUDIT_INTERVAL_MINUTES, TimeUnit.MINUTES);
	}

	public void shutdown() {
		this.executor.shutdownNow();
		EventBus.INSTANCE.removeListener(this);
	}

	public Snapshot getSnapshot() {
		return this.snapshot;
	}

	public Offer getOffer(String atAddress) {
		return this.snapshot.offersByAT.get(atAddress);
	}

	public boolean isCurrentOffer(String atAddress) {
		return atAddress != null && this.snapshot.offersByAT.containsKey(atAddress);
	}

	@Override
	public void listen(Event event) {
		if (event instanceof Controller.OrphanedBlockEvent) {
			BlockData blockData = ((Controller.OrphanedBlockEvent) event).getBlockData();
			requestRestore(blockData.getSignature());
			return;
		}

		if (event instanceof Controller.NewBlockEvent) {
			BlockData blockData = ((Controller.NewBlockEvent) event).getBlockData();
			if (blockData.getHeight() != null)
				requestIncremental(blockData.getHeight());
			return;
		}

	}

	private void requestFullRebuild() {
		synchronized (this.pendingLock) {
			this.fullRebuildPending = true;
			this.minimumPendingHeight = null;
			this.pendingRestoreSignature = null;
		}
	}

	private void requestRestore(byte[] signature) {
		synchronized (this.pendingLock) {
			if (this.fullRebuildPending)
				return;
			this.pendingRestoreSignature = signature == null ? null : signature.clone();
			// Work queued for the orphaned branch is no longer relevant. NewBlockEvent will
			// subsequently queue the first height of the replacement branch.
			this.minimumPendingHeight = null;
		}
	}

	private void requestIncremental(int height) {
		synchronized (this.pendingLock) {
			if (!this.fullRebuildPending && (this.minimumPendingHeight == null || height < this.minimumPendingHeight))
				this.minimumPendingHeight = height;
		}
	}

	void processPendingSafely() {
		if (Synchronizer.getInstance().isSynchronizing())
			return;

		boolean rebuild;
		Integer minimumHeight;
		byte[] restoreSignature;
		synchronized (this.pendingLock) {
			rebuild = this.fullRebuildPending || !this.snapshot.ready;
			minimumHeight = this.minimumPendingHeight;
			restoreSignature = this.pendingRestoreSignature;
			if (!rebuild && minimumHeight == null && restoreSignature == null)
				return;
			this.fullRebuildPending = false;
			this.minimumPendingHeight = null;
			this.pendingRestoreSignature = null;
		}

		try {
			if (rebuild)
				rebuildSnapshot();
			else {
				boolean rebuiltForMissingHistory = restoreSignature != null && !restoreSnapshot(restoreSignature);
				if (rebuiltForMissingHistory)
					rebuildSnapshot();
				if (minimumHeight != null && !rebuiltForMissingHistory)
					refreshSnapshot(minimumHeight);
			}
		} catch (Exception e) {
			LOGGER.warn("Unable to refresh cross-chain offer index; retaining last good snapshot", e);
			synchronized (this.pendingLock) {
				if (rebuild || restoreSignature != null || !this.snapshot.ready)
					this.fullRebuildPending = true;
				else if (minimumHeight != null && (this.minimumPendingHeight == null || minimumHeight < this.minimumPendingHeight))
					this.minimumPendingHeight = minimumHeight;
			}
		}
	}

	private boolean restoreSnapshot(byte[] signature) {
		Snapshot restored = this.snapshotHistory.get(ByteArray.wrap(signature));
		if (restored == null)
			return false;

		this.snapshot = restored;
		LOGGER.debug("Cross-chain offer index restored cached snapshot at height {}", restored.height);
		return true;
	}

	private void rememberSnapshot(Snapshot snapshot) {
		if (snapshot.signature != null)
			this.snapshotHistory.put(ByteArray.wrap(snapshot.signature), snapshot);
	}

	void rebuildSnapshot() throws DataException {
		long startedAt = System.nanoTime();
		Map<ByteArray, Supplier<ACCT>> acctMap = SupportedBlockchain.getAcctMap();
		try (Repository repository = RepositoryManager.getRepository()) {
			List<ATData> atData = repository.getATRepository()
					.getAllATsByFunctionalitySince(acctMap.keySet(), true, null);
			for (ATData definition : atData)
				this.acctDefinitions.put(definition.getATAddress(), definition);
			Map<String, Offer> offers = buildOffers(repository, acctMap, atData);
			BlockData tip = repository.getBlockRepository().getLastBlock();
			Snapshot rebuilt = new Snapshot(offers, tip.getHeight(), tip.getSignature(), true);
			this.snapshot = rebuilt;
			rememberSnapshot(rebuilt);
			LOGGER.info("Cross-chain offer index rebuilt at height {} with {} current offers in {} ms",
					tip.getHeight(), offers.size(), TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt));
		}
	}

	private void refreshSnapshot(int minimumHeight) throws DataException {
		long startedAt = System.nanoTime();
		Snapshot previous = this.snapshot;
		Map<ByteArray, Supplier<ACCT>> acctMap = SupportedBlockchain.getAcctMap();
		try (Repository repository = RepositoryManager.getRepository()) {
			BlockData tip = repository.getBlockRepository().getLastBlock();
			if (tip.getHeight() < previous.height) {
				requestFullRebuild();
				return;
			}

			if (tip.getHeight() - minimumHeight + 1 > MAX_INCREMENTAL_HEIGHTS) {
				rebuildSnapshot();
				return;
			}

			Set<String> executedAddresses = new HashSet<>();
			for (int height = minimumHeight; height <= tip.getHeight(); height++)
				repository.getATRepository().getBlockATStatesAtHeight(height).stream()
						.map(state -> state.getATAddress())
						.forEach(executedAddresses::add);

			Set<String> unknownAddresses = executedAddresses.stream()
					.filter(address -> !this.acctDefinitions.containsKey(address))
					.filter(address -> !this.nonAcctAddresses.contains(address))
					.collect(Collectors.toSet());
			if (!unknownAddresses.isEmpty()) {
				List<ATData> unknownDefinitions = repository.getATRepository()
						.fromATAddresses(new ArrayList<>(unknownAddresses));
				Set<String> returnedAddresses = unknownDefinitions.stream()
						.map(ATData::getATAddress).collect(Collectors.toSet());
				for (ATData definition : unknownDefinitions) {
					if (acctMap.containsKey(ByteArray.wrap(definition.getCodeHash())))
						this.acctDefinitions.put(definition.getATAddress(), definition);
					else
						this.nonAcctAddresses.add(definition.getATAddress());
				}
				unknownAddresses.stream().filter(address -> !returnedAddresses.contains(address))
						.forEach(this.nonAcctAddresses::add);
			}

			List<ATData> changed = executedAddresses.stream()
					.map(this.acctDefinitions::get)
					.filter(java.util.Objects::nonNull)
					.collect(Collectors.toList());
			Set<String> changedAddresses = changed.stream().map(ATData::getATAddress)
					.collect(Collectors.toSet());
			Map<String, Offer> next = new HashMap<>(previous.offersByAT);
			changedAddresses.forEach(next::remove);
			next.putAll(buildOffers(repository, acctMap, changed));
			Snapshot refreshed = new Snapshot(next, tip.getHeight(), tip.getSignature(), true);
			this.snapshot = refreshed;
			rememberSnapshot(refreshed);
			LOGGER.debug("Cross-chain offer index advanced from {} to {} using {} changed ATs in {} ms",
					previous.height, tip.getHeight(), changedAddresses.size(),
					TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt));
		}
	}

	private static Map<String, Offer> buildOffers(Repository repository,
			Map<ByteArray, Supplier<ACCT>> acctMap, List<ATData> atData) throws DataException {
		if (atData.isEmpty())
			return Collections.emptyMap();

		Map<String, ATData> atDataByAddress = atData.stream()
				.collect(Collectors.toMap(ATData::getATAddress, data -> data));
		Map<ByteArray, List<ATData>> byCodeHash = atData.stream()
				.filter(data -> acctMap.containsKey(ByteArray.wrap(data.getCodeHash())))
				.collect(Collectors.groupingBy(data -> ByteArray.wrap(data.getCodeHash())));
		List<CrossChainTradeData> currentOffers = new ArrayList<>();
		for (Map.Entry<ByteArray, List<ATData>> entry : byCodeHash.entrySet()) {
			ACCT acct = acctMap.get(entry.getKey()).get();
			currentOffers.addAll(acct.populateTradeDataList(repository, entry.getValue()).stream()
					.filter(data -> data != null && data.mode == AcctMode.OFFERING)
					.collect(Collectors.toList()));
		}

		currentOffers = TradeBot.getInstance().removeFailedTrades(repository, currentOffers);
		Map<String, Offer> offers = new HashMap<>();
		for (CrossChainTradeData tradeData : currentOffers) {
			ATData definition = atDataByAddress.get(tradeData.qortalAtAddress);
			if (definition != null)
				offers.put(tradeData.qortalAtAddress,
						new Offer(tradeData, definition.getCreatorPublicKey()));
		}
		return offers;
	}
}
