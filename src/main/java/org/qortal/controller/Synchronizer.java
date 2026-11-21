package org.qortal.controller;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.qortal.account.Account;
import org.qortal.account.PublicKeyAccount;
import org.qortal.block.Block;
import org.qortal.block.Block.ValidationResult;
import org.qortal.block.BlockChain;
import org.qortal.data.block.BlockData;
import org.qortal.data.block.BlockSummaryData;
import org.qortal.data.block.CommonBlockData;
import org.qortal.data.transaction.RewardShareTransactionData;
import org.qortal.data.transaction.TransactionData;
import org.qortal.event.Event;
import org.qortal.event.EventBus;
import org.qortal.network.Network;
import org.qortal.network.Peer;
import org.qortal.network.message.*;
import org.qortal.repository.DataException;
import org.qortal.repository.Repository;
import org.qortal.repository.RepositoryManager;
import org.qortal.settings.Settings;
import org.qortal.transaction.Transaction;
import org.qortal.utils.Base58;
import org.qortal.utils.ByteArray;
import org.qortal.utils.NTP;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.qortal.network.Peer.FETCH_BLOCKS_TIMEOUT;
import static org.qortal.network.Peer.SYNC_RESPONSE_TIMEOUT;

public class Synchronizer extends Thread {

	private static final Logger LOGGER = LogManager.getLogger(Synchronizer.class);

	/** Max number of new blocks we aim to add to chain tip in each sync round */
	private static final int SYNC_BATCH_SIZE = 1000; // XXX move to Settings?

	/** Initial jump back of block height when searching for common block with peer */
	private static final int INITIAL_BLOCK_STEP = 8;
	/** Maximum jump back of block height when searching for common block with peer */
	private static final int MAXIMUM_BLOCK_STEP = 128;

	/** Maximum difference in block height between tip and peer's common block before peer is considered TOO DIVERGENT */
	private static final int MAXIMUM_COMMON_DELTA = 240; // XXX move to Settings?

	/** Maximum number of block signatures we ask from peer in one go */
	private static final int MAXIMUM_REQUEST_SIZE = 200; // XXX move to Settings?

	/** Maximum number of consecutive failed sync attempts before marking peer as misbehaved */
	private static final int MAX_CONSECUTIVE_FAILED_SYNC_ATTEMPTS = 3;

    /* Minimum peer version that supports syncing multiple blocks at once via GetBlocksMessage */
    private static final long PEER_VERSION_550 = 0x0500050000L;

	private boolean running;

	/** Latest block signatures from other peers that we know are on inferior chains. */
	List<ByteArray> inferiorChainSignatures = new ArrayList<>();

	/** Recovery mode, which is used to bring back a stalled network */
	private boolean recoveryMode = false;
	private boolean peersAvailable = true; // peersAvailable must default to true
	private long timePeersLastAvailable = 0;

	// Keep track of the size of the last re-org, so it can be logged
	private int lastReorgSize;

	/** Synchronization object for sync variables below */
	public final Object syncLock = new Object();
	/** Whether we are attempting to synchronize. */
	private volatile boolean isSynchronizing = false;
	/** Temporary estimate of synchronization progress for SysTray use. */
	private volatile int syncPercent = 0;
	/** Temporary estimate of blocks remaining for SysTray use. */
	private volatile int blocksRemaining = 0;

	private static volatile boolean requestSync = false;
	private boolean syncRequestPending = false;

	// Keep track of invalid blocks so that we don't keep trying to sync them
	private Map<ByteArray, Long> invalidBlockSignatures = Collections.synchronizedMap(new HashMap<>());
	/** Short request-failure cooldowns keep one slow peer from monopolizing consecutive sync rounds. */
	private final Map<Peer, Long> syncPeerCooldowns = Collections.synchronizedMap(new WeakHashMap<>());
	public Long timeValidBlockLastReceived = null;
	public Long timeInvalidBlockLastReceived = null;

	private static Synchronizer instance;

	public enum SynchronizationResult {
		OK, NOTHING_TO_DO, GENESIS_ONLY, NO_COMMON_BLOCK, TOO_DIVERGENT, NO_REPLY, INFERIOR_CHAIN, INVALID_DATA, NO_BLOCKCHAIN_LOCK, REPOSITORY_ISSUE, SHUTTING_DOWN, CHAIN_TIP_TOO_OLD
    }

	public static class NewChainTipEvent implements Event {
		private final BlockData priorChainTip;
		private final BlockData newChainTip;

		public NewChainTipEvent(BlockData priorChainTip, BlockData newChainTip) {
			this.priorChainTip = priorChainTip;
			this.newChainTip = newChainTip;
		}

		public BlockData getPriorChainTip() {
			return this.priorChainTip;
		}

		public BlockData getNewChainTip() {
			return this.newChainTip;
		}
	}

	// Constructors

	private Synchronizer() {
		this.running = true;
	}

	public static Synchronizer getInstance() {
		if (instance == null) {
			instance = new Synchronizer();
			instance.setPriority(Settings.getInstance().getSynchronizerThreadPriority());

			LOGGER.info("thread priority = " + instance.getPriority());
		}

		return instance;
	}


	@Override
	public void run() {
		Thread.currentThread().setName("Synchronizer");

		if (Settings.getInstance().isLite()) {
			// Lite nodes don't need to sync
			return;
		}

		try {
			while (running && !Controller.isStopping()) {
				Thread.sleep(1000);

				if (requestSync) {
					requestSync = false;
					boolean success = Synchronizer.getInstance().potentiallySynchronize();
					if (!success) {
						// Something went wrong, so try again next time
						requestSync = true;
					}
					// Remember that we have a pending sync request if this attempt failed
					syncRequestPending = !success;
				}
			}
		} catch (InterruptedException e) {
			// Clear interrupted flag so we can shutdown trim threads
			Thread.interrupted();
			// Fall-through to exit
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	public void shutdown() {
		this.running = false;
		this.interrupt();
	}



	public boolean isSynchronizing() {
		return this.isSynchronizing;
	}

	public boolean isSyncRequestPending() {
		return this.syncRequestPending;
	}

	public Integer getSyncPercent() {
		synchronized (this.syncLock) {
			final Long now = NTP.getTime();
			if (now == null)
				return this.isSynchronizing ? this.syncPercent : null;
	
			// Report as 100% synced if the latest block is within the last 60 mins
			final long minLatestBlockTimestamp = now - (60 * 60 * 1000L);
			if (Controller.getInstance().isUpToDate(minLatestBlockTimestamp)) {
				return 100;
			}
	
			return this.isSynchronizing ? this.syncPercent : null;
		}
	}

	public Integer getBlocksRemaining() {
		synchronized (this.syncLock) {
			// Report as 0 blocks remaining if the latest block is within the last 60 mins
			final Long minLatestBlockTimestamp = NTP.getTime() - (60 * 60 * 1000L);
			if (Controller.getInstance().isUpToDate(minLatestBlockTimestamp)) {
				return 0;
			}

			return this.isSynchronizing ? this.blocksRemaining : null;
		}
	}

	public void requestSync() {
		requestSync = true;
	}

	public boolean isSyncRequested() {
		return requestSync;
	}

	public boolean getRecoveryMode() {
		return this.recoveryMode;
	}


	public boolean potentiallySynchronize() throws InterruptedException {
		// Already synchronizing via another thread?
		if (this.isSynchronizing)
			return true;

	

		// Needs a mutable copy of the unmodifiableList
		List<Peer> peers = new ArrayList<>(Network.getInstance().getImmutableHandshakedPeers());
		final int initialPeerCount = peers.size();
		LOGGER.trace(String.format("Starting sync attempt with %d handshaked peer(s)", initialPeerCount));

		// Disregard peers that have "misbehaved" recently
		int beforeCount = peers.size();
		List<Peer> misbehavedPeers = peers.stream().filter(Controller.hasMisbehaved).collect(Collectors.toList());
		peers.removeIf(Controller.hasMisbehaved);
		if (!misbehavedPeers.isEmpty()) {
			LOGGER.trace(String.format("Filtered out %d misbehaved peer(s): %s", misbehavedPeers.size(), 
					misbehavedPeers.stream().map(Peer::toString).collect(Collectors.joining(", "))));
		}

		// Disregard peers that only have genesis block
		beforeCount = peers.size();
		List<Peer> genesisOnlyPeers = peers.stream().filter(Controller.hasOnlyGenesisBlock).collect(Collectors.toList());
		peers.removeIf(Controller.hasOnlyGenesisBlock);
		if (!genesisOnlyPeers.isEmpty()) {
			LOGGER.trace(String.format("Filtered out %d peer(s) with only genesis block: %s", genesisOnlyPeers.size(),
					genesisOnlyPeers.stream().map(Peer::toString).collect(Collectors.joining(", "))));
		}

		// Disregard peers that don't have a recent block
		beforeCount = peers.size();
		List<Peer> noRecentBlockPeers = peers.stream().filter(Controller.hasNoRecentBlock).collect(Collectors.toList());
		peers.removeIf(Controller.hasNoRecentBlock);
		if (!noRecentBlockPeers.isEmpty()) {
			LOGGER.trace(String.format("Filtered out %d peer(s) without recent block: %s", noRecentBlockPeers.size(),
					noRecentBlockPeers.stream().map(Peer::toString).collect(Collectors.joining(", "))));
		}

		// Disregard peers that are on an old version
		beforeCount = peers.size();
		List<Peer> oldVersionPeers = peers.stream().filter(Controller.hasOldVersion).collect(Collectors.toList());
		peers.removeIf(Controller.hasOldVersion);
		if (!oldVersionPeers.isEmpty()) {
			LOGGER.trace(String.format("Filtered out %d peer(s) with old version: %s", oldVersionPeers.size(),
					oldVersionPeers.stream().map(Peer::toString).collect(Collectors.joining(", "))));
		}

		// Prefer another suitable peer briefly after a request timeout. If every peer is
		// cooling down we keep the full set so sparse networks cannot be stalled by this.
		long cooldownNow = System.currentTimeMillis();
		synchronized (this.syncPeerCooldowns) {
			this.syncPeerCooldowns.entrySet().removeIf(entry -> entry.getValue() <= cooldownNow);
		}
		List<Peer> peersOutsideCooldown = peers.stream()
				.filter(peer -> !this.syncPeerCooldowns.containsKey(peer)).collect(Collectors.toList());
		if (peersOutsideCooldown.size() >= Settings.getInstance().getMinBlockchainPeers())
			peers = peersOutsideCooldown;

		checkRecoveryModeForPeers(peers);

		// Check we have enough peers to potentially synchronize
		final int minBlockchainPeers = Settings.getInstance().getMinBlockchainPeers();
		if (peers.size() < minBlockchainPeers) {
			LOGGER.trace(String.format("Not enough peers for sync. Required: %d, Available: %d (filtered from %d handshaked peers)", 
					minBlockchainPeers, peers.size(), initialPeerCount));
			return true;
		}

		// Disregard peers that have no block signature or the same block signature as us
		beforeCount = peers.size();
		List<Peer> noOrSameBlockPeers = peers.stream().filter(Controller.hasNoOrSameBlock).collect(Collectors.toList());
		peers.removeIf(Controller.hasNoOrSameBlock);
		if (!noOrSameBlockPeers.isEmpty()) {
			LOGGER.trace(String.format("Filtered out %d peer(s) with no or same block signature: %s", noOrSameBlockPeers.size(),
					noOrSameBlockPeers.stream().map(Peer::toString).collect(Collectors.joining(", "))));
		}

		// Disregard peers that are on the same block as last sync attempt and we didn't like their chain
		beforeCount = peers.size();
		List<Peer> inferiorChainPeers = peers.stream().filter(Controller.hasInferiorChainTip).collect(Collectors.toList());
		peers.removeIf(Controller.hasInferiorChainTip);
		if (!inferiorChainPeers.isEmpty()) {
			LOGGER.trace(String.format("Filtered out %d peer(s) with inferior chain tip: %s", inferiorChainPeers.size(),
					inferiorChainPeers.stream().map(Peer::toString).collect(Collectors.joining(", "))));
		}

		// Disregard peers that have a block with an invalid signer
		beforeCount = peers.size();
		List<Peer> invalidSignerPeers = peers.stream().filter(Controller.hasInvalidSigner).collect(Collectors.toList());
		peers.removeIf(Controller.hasInvalidSigner);
		if (!invalidSignerPeers.isEmpty()) {
			LOGGER.trace(String.format("Filtered out %d peer(s) with invalid signer: %s", invalidSignerPeers.size(),
					invalidSignerPeers.stream().map(Peer::toString).collect(Collectors.joining(", "))));
		}

		final int peersBeforeComparison = peers.size();

		// Request recent block summaries from the remaining peers, and locate our common block with each
		Synchronizer.getInstance().findCommonBlocksWithPeers(peers);

		// Compare the peers against each other, and against our chain, which will return an updated list excluding those without common blocks
		peers = Synchronizer.getInstance().comparePeers(peers);

		// We may have added more inferior chain tips when comparing peers, so remove any peers that are currently on those chains
		peers.removeIf(Controller.hasInferiorChainTip);

		// Remove any peers that are no longer on a recent block since the last check
		peers.removeIf(Controller.hasNoRecentBlock);

		final int peersRemoved = peersBeforeComparison - peers.size();
		if (peersRemoved > 0 && !peers.isEmpty())
			LOGGER.debug(String.format("Ignoring %d peers on inferior chains. Peers remaining: %d", peersRemoved, peers.size()));

		if (peers.isEmpty()) {
			LOGGER.trace(String.format("No suitable peers available for synchronization after filtering. Started with %d handshaked peer(s), filtered down to 0", initialPeerCount));
			return true;
		}

		if (peers.size() > 1) {
			StringBuilder finalPeersString = new StringBuilder();
			for (Peer peer : peers)
				finalPeersString = finalPeersString.length() > 0 ? finalPeersString.append(", ").append(peer) : finalPeersString.append(peer);
			LOGGER.debug(String.format("Choosing random peer from: [%s]", finalPeersString.toString()));
		}

		// Pick random peer to sync with
		int index = new SecureRandom().nextInt(peers.size());
		Peer peer = peers.get(index);
		
	

		SynchronizationResult syncResult = actuallySynchronize(peer, false);
		if (syncResult == SynchronizationResult.NO_REPLY) {
			int failures = Math.max(1, peer.getPeerData().getFailedSyncCount());
			long delay = Math.min(60_000L, 5_000L << Math.min(failures - 1, 3));
			this.syncPeerCooldowns.put(peer, System.currentTimeMillis() + delay);
		} else if (syncResult == SynchronizationResult.OK || syncResult == SynchronizationResult.NOTHING_TO_DO) {
			this.syncPeerCooldowns.remove(peer);
		}
		if (syncResult == SynchronizationResult.NO_BLOCKCHAIN_LOCK) {
			// No blockchain lock - force a retry by returning false
			return false;
		}

		return true;
	}

	public SynchronizationResult actuallySynchronize(Peer peer, boolean force) throws InterruptedException {
		boolean hasStatusChanged = false;
		BlockData priorChainTip = Controller.getInstance().getChainTip();

		synchronized (this.syncLock) {
			this.syncPercent = (priorChainTip.getHeight() * 100) / peer.getChainTipData().getHeight();

			// Only update SysTray if we're potentially changing height
			if (this.syncPercent < 100) {
				this.isSynchronizing = true;
				hasStatusChanged = true;
			}
		}
		peer.setSyncInProgress(true);

		if (hasStatusChanged)
			Controller.getInstance().updateSysTray();

		try {
			SynchronizationResult syncResult = Synchronizer.getInstance().synchronize(peer, force);
			switch (syncResult) {
				case GENESIS_ONLY:
				case NO_COMMON_BLOCK:
				case TOO_DIVERGENT:
				case INVALID_DATA: {
					// These are more serious results that warrant a cool-off
					LOGGER.info(String.format("Failed to synchronize with peer %s (%s) - cooling off", peer, syncResult.name()));

					// Don't use this peer again for a while
					Network.getInstance().peerMisbehaved(peer);
					break;
				}

				case INFERIOR_CHAIN: {
					// Update our list of inferior chain tips
					ByteArray inferiorChainSignature = ByteArray.wrap(peer.getChainTipData().getSignature());
					if (!inferiorChainSignatures.contains(inferiorChainSignature))
						inferiorChainSignatures.add(inferiorChainSignature);

					// These are minor failure results so fine to try again
					LOGGER.debug(() -> String.format("Refused to synchronize with peer %s (%s)", peer, syncResult.name()));

					// Notify peer of our superior chain
					Message message = Network.getInstance().buildHeightOrChainTipInfo(peer);
					if (message == null || !peer.sendMessage(message))
						peer.disconnect("failed to notify peer of our superior chain");
					break;
				}

				case NO_REPLY:
				case NO_BLOCKCHAIN_LOCK:
				case REPOSITORY_ISSUE:
				case CHAIN_TIP_TOO_OLD:
					// These are minor failure results so fine to try again
					LOGGER.debug(() -> String.format("Failed to synchronize with peer %s (%s)", peer, syncResult.name()));
					break;

				case SHUTTING_DOWN:
					// Just quietly exit
					break;

				case OK:
					// fall-through...
				case NOTHING_TO_DO: {
					// Update our list of inferior chain tips
					ByteArray inferiorChainSignature = ByteArray.wrap(peer.getChainTipData().getSignature());
					if (!inferiorChainSignatures.contains(inferiorChainSignature))
						inferiorChainSignatures.add(inferiorChainSignature);

					LOGGER.debug(() -> String.format("Synchronized with peer %s (%s)", peer, syncResult.name()));
					break;
				}
			}

			if (!running) {
				// We've stopped
				return SynchronizationResult.SHUTTING_DOWN;
			}

			// Has our chain tip changed?
			BlockData newChainTip;

			try (final Repository repository = RepositoryManager.getRepository()) {
				newChainTip = repository.getBlockRepository().getLastBlock();
			} catch (DataException e) {
				LOGGER.warn(String.format("Repository issue when trying to fetch post-synchronization chain tip: %s", e.getMessage()));
				return syncResult;
			}

			if (!Arrays.equals(newChainTip.getSignature(), priorChainTip.getSignature())) {
				// Reset our cache of inferior chains
				inferiorChainSignatures.clear();

				Network.getInstance().broadcastOurChain();

				EventBus.INSTANCE.notify(new NewChainTipEvent(priorChainTip, newChainTip));
			}

			return syncResult;
		} finally {
			this.isSynchronizing = false;
			peer.setSyncInProgress(false);
		}
	}

	private boolean checkRecoveryModeForPeers(List<Peer> qualifiedPeers) {
		List<Peer> handshakedPeers = Network.getInstance().getImmutableHandshakedPeers();

		if (!handshakedPeers.isEmpty()) {
			// There is at least one handshaked peer
			if (qualifiedPeers.isEmpty()) {
				// There are no 'qualified' peers - i.e. peers that have a recent block we can sync to
				boolean werePeersAvailable = peersAvailable;
				peersAvailable = false;

				// If peers only just became unavailable, update our record of the time they were last available
				if (werePeersAvailable)
					timePeersLastAvailable = NTP.getTime();

				// If enough time has passed, enter recovery mode, which lifts some restrictions on who we can sync with and when we can mint
				long recoveryModeTimeout = Settings.getInstance().getRecoveryModeTimeout();
				if (NTP.getTime() - timePeersLastAvailable > recoveryModeTimeout) {
					if (!recoveryMode) {
						LOGGER.info(String.format("Peers have been unavailable for %d minutes. Entering recovery mode...", recoveryModeTimeout/60/1000));
						recoveryMode = true;
					}
				}
			} else {
				// We now have at least one peer with a recent block, so we can exit recovery mode and sync normally
				peersAvailable = true;
				if (recoveryMode) {
					LOGGER.info("Peers have become available again. Exiting recovery mode...");
					recoveryMode = false;
				}
			}
		}
		return recoveryMode;
	}

	public void addInferiorChainSignature(byte[] inferiorSignature) {
		// Update our list of inferior chain tips
		ByteArray inferiorChainSignature = ByteArray.wrap(inferiorSignature);
		if (!inferiorChainSignatures.contains(inferiorChainSignature))
			inferiorChainSignatures.add(inferiorChainSignature);
	}


	/**
	 * Iterate through a list of supplied peers, and attempt to find our common block with each.
	 * If a common block is found, its summary will be retained in the peer's commonBlockSummary property, for processing later.
	 * <p>
	 * Will return <tt>SynchronizationResult.OK</tt> on success.
	 * <p>
	 * @param peers
	 * @return SynchronizationResult.OK if the process completed successfully, or a different SynchronizationResult if something went wrong.
	 * @throws InterruptedException
	 */
	public SynchronizationResult findCommonBlocksWithPeers(List<Peer> peers) throws InterruptedException {
		try (final Repository repository = RepositoryManager.getRepository()) {
			try {

				if (peers.isEmpty())
					return SynchronizationResult.NOTHING_TO_DO;

				// If our latest block is very old, it's best that we don't try and determine the best peers to sync to.
				// This is because it can involve very large chain comparisons, which is too intensive.
				// In reality, most forking problems occur near the chain tips, so we will reserve this functionality for those situations.
				final Long minLatestBlockTimestamp = Controller.getMinimumLatestBlockTimestamp();
				if (minLatestBlockTimestamp == null)
					return SynchronizationResult.REPOSITORY_ISSUE;

				final BlockData ourLatestBlockData = repository.getBlockRepository().getLastBlock();
				if (ourLatestBlockData.getTimestamp() < minLatestBlockTimestamp) {
					LOGGER.debug(String.format("Our latest block is very old, so we won't collect common block info from peers"));
					return SynchronizationResult.NOTHING_TO_DO;
				}

				LOGGER.debug(String.format("Searching for common blocks with %d peers...", peers.size()));
				final long startTime = System.currentTimeMillis();
				int commonBlocksFound = 0;
				boolean wereNewRequestsMade = false;

				for (Peer peer : peers) {
					// Are we shutting down?
					if (Controller.isStopping())
						return SynchronizationResult.SHUTTING_DOWN;

					// Check if we can use the cached common block data, by comparing the peer's current chain tip against the peer's chain tip when we last found our common block
					if (peer.canUseCachedCommonBlockData()) {
						LOGGER.debug(String.format("Skipping peer %s because we already have the latest common block data in our cache. Cached common block sig is %.08s", peer, Base58.encode(peer.getCommonBlockData().getCommonBlockSummary().getSignature())));
						commonBlocksFound++;
						continue;
					}

					// Cached data is stale, so clear it and repopulate
					peer.setCommonBlockData(null);

					// Search for the common block
					Synchronizer.getInstance().findCommonBlockWithPeer(peer, repository);
					if (peer.getCommonBlockData() != null)
						commonBlocksFound++;

					// This round wasn't served entirely from the cache, so we may want to log the results
					wereNewRequestsMade = true;
				}

				if (wereNewRequestsMade) {
					final long totalTimeTaken = System.currentTimeMillis() - startTime;
					LOGGER.debug(String.format("Finished searching for common blocks with %d peer%s. Found: %d. Total time taken: %d ms", peers.size(), (peers.size() != 1 ? "s" : ""), commonBlocksFound, totalTimeTaken));
				}

				return SynchronizationResult.OK;
			} finally {
				repository.discardChanges(); // Free repository locks, if any, also in case anything went wrong
			}
		} catch (DataException e) {
			LOGGER.error("Repository issue during synchronization with peer", e);
			return SynchronizationResult.REPOSITORY_ISSUE;
		}
	}

	/**
	 * Attempt to find the find our common block with supplied peer.
	 * If a common block is found, its summary will be retained in the peer's commonBlockSummary property, for processing later.
	 * <p>
	 * Will return <tt>SynchronizationResult.OK</tt> on success.
	 * <p>
	 * @param peer
	 * @param repository
	 * @return SynchronizationResult.OK if the process completed successfully, or a different SynchronizationResult if something went wrong.
	 * @throws InterruptedException
	 */
	public SynchronizationResult findCommonBlockWithPeer(Peer peer, Repository repository) throws InterruptedException {
		try {
			final BlockData ourLatestBlockData = repository.getBlockRepository().getLastBlock();
			final int ourInitialHeight = ourLatestBlockData.getHeight();

			BlockSummaryData peerChainTipData = peer.getChainTipData();
			int peerHeight = peerChainTipData.getHeight();
			byte[] peersLastBlockSignature = peerChainTipData.getSignature();

			byte[] ourLastBlockSignature = ourLatestBlockData.getSignature();
			LOGGER.debug(String.format("Fetching summaries from peer %s at height %d, sig %.8s, ts %d; our height %d, sig %.8s, ts %d", peer,
					peerHeight, Base58.encode(peersLastBlockSignature), peerChainTipData.getTimestamp(),
					ourInitialHeight, Base58.encode(ourLastBlockSignature), ourLatestBlockData.getTimestamp()));

			List<BlockSummaryData> peerBlockSummaries = new ArrayList<>();
			SynchronizationResult findCommonBlockResult = fetchSummariesFromCommonBlock(repository, peer, ourInitialHeight, false, peerBlockSummaries, false);
			if (findCommonBlockResult != SynchronizationResult.OK) {
				// Logging performed by fetchSummariesFromCommonBlock() above
				peer.setCommonBlockData(null);
				return findCommonBlockResult;
			}

			// First summary is common block
			final BlockData commonBlockData = repository.getBlockRepository().fromSignature(peerBlockSummaries.get(0).getSignature());
			final BlockSummaryData commonBlockSummary = new BlockSummaryData(commonBlockData);
			final int commonBlockHeight = commonBlockData.getHeight();
			final byte[] commonBlockSig = commonBlockData.getSignature();
			final String commonBlockSig58 = Base58.encode(commonBlockSig);
			LOGGER.debug(String.format("Common block with peer %s is at height %d, sig %.8s, ts %d", peer,
					commonBlockHeight, commonBlockSig58, commonBlockData.getTimestamp()));
			peerBlockSummaries.remove(0);

			// Store the common block summary against the peer, and the current chain tip (for caching)
			peer.setCommonBlockData(new CommonBlockData(commonBlockSummary, peerChainTipData));

			return SynchronizationResult.OK;
		} catch (DataException e) {
			LOGGER.error("Repository issue during synchronization with peer", e);
			return SynchronizationResult.REPOSITORY_ISSUE;
		}
	}


	/**
	 * Compare a list of peers to determine the best peer(s) to sync to next.
	 * <p>
	 * Will return a filtered list of peers on success, or an identical list of peers on failure.
	 * This allows us to fall back to legacy behaviour (random selection from the entire list of peers), if we are unable to make the comparison.
	 * <p>
	 * @param peers
	 * @return a list of peers, possibly filtered.
	 * @throws InterruptedException
	 */
	public List<Peer> comparePeers(List<Peer> peers) throws InterruptedException {
		try (final Repository repository = RepositoryManager.getRepository()) {
			try {

				// If our latest block is very old, it's best that we don't try and determine the best peers to sync to.
				// This is because it can involve very large chain comparisons, which is too intensive.
				// In reality, most forking problems occur near the chain tips, so we will reserve this functionality for those situations.
				Long minLatestBlockTimestamp = Controller.getMinimumLatestBlockTimestamp();
				if (minLatestBlockTimestamp == null)
					return peers;

				final BlockData ourLatestBlockData = repository.getBlockRepository().getLastBlock();
				if (ourLatestBlockData.getTimestamp() < minLatestBlockTimestamp) {
					LOGGER.debug(String.format("Our latest block is very old, so we won't filter the peers list"));
					return peers;
				}

				// We will switch to a new chain weight consensus algorithm at a hard fork, so determine if this has happened yet
				boolean usingSameLengthChainWeight = (NTP.getTime() >= BlockChain.getInstance().getCalcChainWeightTimestamp());
				LOGGER.debug(String.format("Using %s chain weight consensus algorithm", (usingSameLengthChainWeight ? "same-length" : "variable-length")));

				// Retrieve a list of unique common blocks from this list of peers
				List<BlockSummaryData> commonBlocks = this.uniqueCommonBlocks(peers);

				// Order common blocks by height, in ascending order
				// This is essential for the logic below to make the correct decisions when discarding chains - do not remove
				commonBlocks.sort((b1, b2) -> Integer.valueOf(b1.getHeight()).compareTo(Integer.valueOf(b2.getHeight())));

				// Get our latest height
				final int ourHeight = ourLatestBlockData.getHeight();

				// Create a placeholder to track of common blocks that we can discard due to being inferior chains
				int dropPeersAfterCommonBlockHeight = 0;

				NumberFormat accurateFormatter = new DecimalFormat("0.################E0");

				// Remove peers with no common block data
				Iterator iterator = peers.iterator();
				while (iterator.hasNext()) {
					Peer peer = (Peer) iterator.next();
					if (peer.getCommonBlockData() == null) {
						LOGGER.debug(String.format("Removed peer %s because it has no common block data", peer));
						iterator.remove();
					}
				}

				// Loop through each group of common blocks
				for (BlockSummaryData commonBlockSummary : commonBlocks) {
					List<Peer> peersSharingCommonBlock = peers.stream().filter(peer -> peer.getCommonBlockData().getCommonBlockSummary().equals(commonBlockSummary)).collect(Collectors.toList());

					// Check if we need to discard this group of peers
					if (dropPeersAfterCommonBlockHeight > 0) {
						if (commonBlockSummary.getHeight() > dropPeersAfterCommonBlockHeight) {
							// We have already determined that the correct chain diverged from a lower height. We are safe to skip these peers.
							for (Peer peer : peersSharingCommonBlock) {
								LOGGER.debug(String.format("Peer %s has common block at height %d but the superior chain is at height %d. Removing it from this round.", peer, commonBlockSummary.getHeight(), dropPeersAfterCommonBlockHeight));
								//this.addInferiorChainSignature(peer.getChainTipData().getLastBlockSignature());
							}
							continue;
						}
					}

					// Calculate the length of the shortest peer chain sharing this common block, including our chain
					final int ourAdditionalBlocksAfterCommonBlock = ourHeight - commonBlockSummary.getHeight();
					int minChainLength = this.calculateMinChainLengthOfPeers(peersSharingCommonBlock, commonBlockSummary);

					// Fetch block summaries from each peer
					Iterator peersSharingCommonBlockIterator = peersSharingCommonBlock.iterator();
					while (peersSharingCommonBlockIterator.hasNext()) {
						Peer peer = (Peer) peersSharingCommonBlockIterator.next();

						// If we're shutting down, just return the latest peer list
						if (Controller.isStopping())
							return peers;

						// Count the number of blocks this peer has beyond our common block
						final BlockSummaryData peerChainTipData = peer.getChainTipData();
						final int peerHeight = peerChainTipData.getHeight();
						final byte[] peerLastBlockSignature = peerChainTipData.getSignature();
						final int peerAdditionalBlocksAfterCommonBlock = peerHeight - commonBlockSummary.getHeight();
						// Limit the number of blocks we are comparing. FUTURE: we could request more in batches, but there may not be a case when this is needed
						int summariesRequired = Math.min(peerAdditionalBlocksAfterCommonBlock, MAXIMUM_REQUEST_SIZE);

						// Check if we can use the cached common block summaries, by comparing the peer's current chain tip against the peer's chain tip when we last found our common block
						boolean useCachedSummaries = false;
						if (peer.canUseCachedCommonBlockData()) {
							if (peer.getCommonBlockData().getBlockSummariesAfterCommonBlock() != null) {
								if (peer.getCommonBlockData().getBlockSummariesAfterCommonBlock().size() == summariesRequired) {
									LOGGER.trace(String.format("Using cached block summaries for peer %s", peer));
									useCachedSummaries = true;
								}
							}
						}

						if (!useCachedSummaries) {
							if (summariesRequired > 0) {
								LOGGER.trace(String.format("Requesting %d block summar%s from peer %s after common block %.8s. Peer height: %d", summariesRequired, (summariesRequired != 1 ? "ies" : "y"), peer, Base58.encode(commonBlockSummary.getSignature()), peerHeight));

								// Forget any cached summaries
								peer.getCommonBlockData().setBlockSummariesAfterCommonBlock(null);

								// Request new block summaries
								List<BlockSummaryData> blockSummaries = this.getBlockSummaries(peer, commonBlockSummary.getSignature(), summariesRequired);
								if (blockSummaries != null) {
									LOGGER.trace(String.format("Peer %s returned %d block summar%s", peer, blockSummaries.size(), (blockSummaries.size() != 1 ? "ies" : "y")));

									if (blockSummaries.size() < summariesRequired)
										// This could mean that the peer has re-orged. Exclude this peer until they return the summaries we expect.
										LOGGER.debug(String.format("Peer %s returned %d block summar%s instead of expected %d - excluding them from this round", peer, blockSummaries.size(), (blockSummaries.size() != 1 ? "ies" : "y"), summariesRequired));
									else if (blockSummaryWithSignature(peerLastBlockSignature, blockSummaries) == null)
										// We don't have a block summary for the peer's reported chain tip, so should exclude it
										LOGGER.debug(String.format("Peer %s didn't return a block summary with signature %.8s - excluding them from this round", peer, Base58.encode(peerLastBlockSignature)));
									else
										// All looks good, so store the retrieved block summaries in the peer's cache
										peer.getCommonBlockData().setBlockSummariesAfterCommonBlock(blockSummaries);
								}
							} else {
								// There are no block summaries after this common block
								peer.getCommonBlockData().setBlockSummariesAfterCommonBlock(null);
							}
						}

						// Ignore this peer if it holds an invalid block
						if (this.containsInvalidBlockSummary(peer.getCommonBlockData().getBlockSummariesAfterCommonBlock())) {
							LOGGER.debug("Ignoring peer %s because it holds an invalid block", peer);
							peers.remove(peer);
							peersSharingCommonBlockIterator.remove();
							continue;
						}

						// Reduce minChainLength if needed. If we don't have any blocks, this peer will be excluded from chain weight comparisons later in the process, so we shouldn't update minChainLength
						List <BlockSummaryData> peerBlockSummaries = peer.getCommonBlockData().getBlockSummariesAfterCommonBlock();
						if (peerBlockSummaries != null && !peerBlockSummaries.isEmpty())
							if (peerBlockSummaries.size() < minChainLength)
								minChainLength = peerBlockSummaries.size();
					}

					// Fetch our corresponding block summaries. Limit to MAXIMUM_REQUEST_SIZE, in order to make the comparison fairer, as peers have been limited too
					final int ourSummariesRequired = Math.min(ourAdditionalBlocksAfterCommonBlock, MAXIMUM_REQUEST_SIZE);
					LOGGER.trace(String.format("About to fetch our block summaries from %d to %d. Our height: %d", commonBlockSummary.getHeight() + 1, commonBlockSummary.getHeight() + ourSummariesRequired, ourHeight));
					List<BlockSummaryData> ourBlockSummaries = repository.getBlockRepository().getBlockSummaries(commonBlockSummary.getHeight() + 1, commonBlockSummary.getHeight() + ourSummariesRequired);
					if (ourBlockSummaries.isEmpty()) {
						LOGGER.debug(String.format("We don't have any block summaries so can't compare our chain against peers with this common block. We can still compare them against each other."));
					}
					else {
						populateBlockSummariesMinterLevels(repository, ourBlockSummaries);
						// Reduce minChainLength if we have less summaries
						if (ourBlockSummaries.size() < minChainLength)
							minChainLength = ourBlockSummaries.size();
					}

					// Create array to hold peers for comparison
					List<Peer> superiorPeersForComparison = new ArrayList<>();

					// Calculate max height for chain weight comparisons
					int maxHeightForChainWeightComparisons = commonBlockSummary.getHeight() + minChainLength;

					// Calculate our chain weight
					BigInteger ourChainWeight = BigInteger.valueOf(0);
					if (!ourBlockSummaries.isEmpty())
						ourChainWeight = Block.calcChainWeight(commonBlockSummary.getHeight(), commonBlockSummary.getSignature(), ourBlockSummaries, maxHeightForChainWeightComparisons);

					LOGGER.debug(String.format("Our chain weight based on %d blocks is %s", (usingSameLengthChainWeight ? minChainLength : ourBlockSummaries.size()), accurateFormatter.format(ourChainWeight)));

					LOGGER.debug(String.format("Listing peers with common block %.8s...", Base58.encode(commonBlockSummary.getSignature())));
					for (Peer peer : peersSharingCommonBlock) {
						BlockSummaryData peerChainTipData = peer.getChainTipData();
						final int peerHeight = peerChainTipData.getHeight();
						final Long peerLastBlockTimestamp = peerChainTipData.getTimestamp();
						final int peerAdditionalBlocksAfterCommonBlock = peerHeight - commonBlockSummary.getHeight();
						final CommonBlockData peerCommonBlockData = peer.getCommonBlockData();

						if (peerCommonBlockData == null || peerCommonBlockData.getBlockSummariesAfterCommonBlock() == null || peerCommonBlockData.getBlockSummariesAfterCommonBlock().isEmpty()) {
							// No response - remove this peer for now
							LOGGER.debug(String.format("Peer %s doesn't have any block summaries - removing it from this round", peer));
							peers.remove(peer);
							continue;
						}

						// If peer is our of date (since our last check), we should exclude it from this round
						minLatestBlockTimestamp = Controller.getMinimumLatestBlockTimestamp();
						if (peerLastBlockTimestamp == null || peerLastBlockTimestamp < minLatestBlockTimestamp) {
							LOGGER.debug(String.format("Peer %s is out of date - removing it from this round", peer));
							peers.remove(peer);
							continue;
						}

						final List<BlockSummaryData> peerBlockSummariesAfterCommonBlock = peerCommonBlockData.getBlockSummariesAfterCommonBlock();
						populateBlockSummariesMinterLevels(repository, peerBlockSummariesAfterCommonBlock);

						// Calculate cumulative chain weight of this blockchain subset, from common block to highest mutual block held by all peers in this group.
						LOGGER.debug(String.format("About to calculate chain weight based on %d blocks for peer %s with common block %.8s (peer has %d blocks after common block)", (usingSameLengthChainWeight ? minChainLength : peerBlockSummariesAfterCommonBlock.size()), peer, Base58.encode(commonBlockSummary.getSignature()), peerAdditionalBlocksAfterCommonBlock));
						BigInteger peerChainWeight = Block.calcChainWeight(commonBlockSummary.getHeight(), commonBlockSummary.getSignature(), peerBlockSummariesAfterCommonBlock, maxHeightForChainWeightComparisons);
						peer.getCommonBlockData().setChainWeight(peerChainWeight);
						LOGGER.debug(String.format("Chain weight of peer %s based on %d blocks (%d - %d) is %s", peer, (usingSameLengthChainWeight ? minChainLength : peerBlockSummariesAfterCommonBlock.size()), peerBlockSummariesAfterCommonBlock.get(0).getHeight(), peerBlockSummariesAfterCommonBlock.get(peerBlockSummariesAfterCommonBlock.size()-1).getHeight(), accurateFormatter.format(peerChainWeight)));

						// Compare against our chain - if our blockchain has greater weight then don't synchronize with peer (or any others in this group)
						if (ourChainWeight.compareTo(peerChainWeight) > 0) {
							// This peer is on an inferior chain - remove it
							LOGGER.debug(String.format("Peer %s is on an inferior chain to us - removing it from this round", peer));
							peers.remove(peer);
						}
						else {
							// Our chain is inferior or equal
							LOGGER.debug(String.format("Peer %s is on an equal or better chain to us. We will compare the other peers sharing this common block against each other, and drop all peers sharing higher common blocks.", peer));
							dropPeersAfterCommonBlockHeight = commonBlockSummary.getHeight();
							superiorPeersForComparison.add(peer);
						}
					}

					// Now that we have selected the best peers, compare them against each other and remove any with lower weights
					if (!superiorPeersForComparison.isEmpty()) {
						BigInteger bestChainWeight = null;
						for (Peer peer : superiorPeersForComparison) {
							// Increase bestChainWeight if needed
							if (bestChainWeight == null || peer.getCommonBlockData().getChainWeight().compareTo(bestChainWeight) >= 0)
								bestChainWeight = peer.getCommonBlockData().getChainWeight();
						}
						for (Peer peer : superiorPeersForComparison) {
							// Check if we should discard an inferior peer
							if (peer.getCommonBlockData().getChainWeight().compareTo(bestChainWeight) < 0) {
								BigInteger difference = bestChainWeight.subtract(peer.getCommonBlockData().getChainWeight());
								LOGGER.debug(String.format("Peer %s has a lower chain weight (difference: %s) than other peer(s) in this group - removing it from this round.", peer, accurateFormatter.format(difference)));
								peers.remove(peer);
							}
						}
						// FUTURE: we may want to prefer peers with additional blocks, and compare the additional blocks against each other.
						// This would fast track us to the best candidate for the latest block.
						// Right now, peers with the exact same chain as us are treated equally to those with an additional block.
					}
				}

				return peers;
			} finally {
				repository.discardChanges(); // Free repository locks, if any, also in case anything went wrong
			}
		} catch (DataException e) {
			LOGGER.error("Repository issue during peer comparison", e);
			return peers;
		}
	}

	private List<BlockSummaryData> uniqueCommonBlocks(List<Peer> peers) {
		List<BlockSummaryData> commonBlocks = new ArrayList<>();

		for (Peer peer : peers) {
			if (peer.getCommonBlockData() != null && peer.getCommonBlockData().getCommonBlockSummary() != null) {
				LOGGER.trace(String.format("Peer %s has common block %.8s", peer, Base58.encode(peer.getCommonBlockData().getCommonBlockSummary().getSignature())));

				BlockSummaryData commonBlockSummary = peer.getCommonBlockData().getCommonBlockSummary();
				if (!commonBlocks.contains(commonBlockSummary))
					commonBlocks.add(commonBlockSummary);
			}
			else {
				LOGGER.trace(String.format("Peer %s has no common block data. Skipping...", peer));
			}
		}

		return commonBlocks;
	}

	private int calculateMinChainLengthOfPeers(List<Peer> peersSharingCommonBlock, BlockSummaryData commonBlockSummary) {
		// Calculate the length of the shortest peer chain sharing this common block
		int minChainLength = 0;
		for (Peer peer : peersSharingCommonBlock) {
			final int peerHeight = peer.getChainTipData().getHeight();
			final int peerAdditionalBlocksAfterCommonBlock = peerHeight - commonBlockSummary.getHeight();

			if (peerAdditionalBlocksAfterCommonBlock < minChainLength || minChainLength == 0)
				minChainLength = peerAdditionalBlocksAfterCommonBlock;
		}
		return minChainLength;
	}

	private BlockSummaryData blockSummaryWithSignature(byte[] signature, List<BlockSummaryData> blockSummaries) {
		if (blockSummaries != null)
			return blockSummaries.stream().filter(blockSummary -> Arrays.equals(blockSummary.getSignature(), signature)).findAny().orElse(null);
		return null;
	}



	/* Invalid block signature tracking */

	public Map<ByteArray, Long> getInvalidBlockSignatures() {
		return this.invalidBlockSignatures;
	}

	private void addInvalidBlockSignature(byte[] signature) {
		Long now = NTP.getTime();
		if (now == null) {
			return;
		}

		// Add or update existing entry
		invalidBlockSignatures.put(ByteArray.wrap(signature), now);
	}
	private void deleteOlderInvalidSignatures(Long now) {
		if (now == null) {
			return;
		}

		// Delete signatures with older timestamps
		Iterator it = invalidBlockSignatures.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry)it.next();
			Long lastSeen = (Long) pair.getValue();

			// Remove signature if we haven't seen it for more than 1 hour
			if (now - lastSeen > 60 * 60 * 1000L) {
				it.remove();
			}
		}
	}
	public boolean containsInvalidBlockSummary(List<BlockSummaryData> blockSummaries) {
		if (blockSummaries == null || invalidBlockSignatures == null) {
			return false;
		}

		// Loop through our known invalid blocks and check each one against supplied block summaries
		for (ByteArray invalidSignature : invalidBlockSignatures.keySet()) {
			for (BlockSummaryData blockSummary : blockSummaries) {
				byte[] signature = blockSummary.getSignature();
				if (Arrays.equals(signature, invalidSignature.value)) {
					return true;
				}
			}
		}
		return false;
	}
	private boolean containsInvalidBlockSignature(List<byte[]> blockSignatures) {
		if (blockSignatures == null || invalidBlockSignatures == null) {
			return false;
		}

		// Loop through our known invalid blocks and check each one against supplied block signatures
		for (ByteArray invalidSignature : invalidBlockSignatures.keySet()) {
			for (byte[] signature : blockSignatures) {
				if (Arrays.equals(signature, invalidSignature.value)) {
					return true;
				}
			}
		}
		return false;
	}


	/**
	 * Attempt to synchronize blockchain with peer.
	 * <p>
	 * Will return <tt>true</tt> if synchronization succeeded,
	 * even if no changes were made to our blockchain.
	 * <p>
	 * @param peer
	 * @return false if something went wrong, true otherwise.
	 * @throws InterruptedException
	 */
	public SynchronizationResult synchronize(Peer peer, boolean force) throws InterruptedException {
		ReentrantLock blockchainLock = Controller.getInstance().getBlockchainLock();

		try (final Repository repository = RepositoryManager.getRepository()) {
			try {
				// Capture the exact local tip used to build this plan. Network work is deliberately
				// performed without the blockchain lock; the signature is rechecked after locking.
				final BlockData ourLatestBlockData = repository.getBlockRepository().getLastBlock();
				final int ourInitialHeight = ourLatestBlockData.getHeight();
				final byte[] ourInitialSignature = ourLatestBlockData.getSignature();

				BlockSummaryData peerChainTipData = peer.getChainTipData();
				if (peerChainTipData == null)
					return SynchronizationResult.NO_REPLY;
				int peerHeight = peerChainTipData.getHeight();
				byte[] peersLastBlockSignature = peerChainTipData.getSignature();

				LOGGER.info(String.format("Synchronizing with peer %s at height %d, sig %.8s, ts %d; our height %d, sig %.8s, ts %d", peer,
						peerHeight, Base58.encode(peersLastBlockSignature), peerChainTipData.getTimestamp(),
						ourInitialHeight, Base58.encode(ourInitialSignature), ourLatestBlockData.getTimestamp()));

				this.lastReorgSize = 0;
				Long now = NTP.getTime();
				if (this.timeValidBlockLastReceived == null)
					this.timeValidBlockLastReceived = now;
				this.deleteOlderInvalidSignatures(now);

				List<BlockSummaryData> peerBlockSummaries = new ArrayList<>();
				SynchronizationResult commonResult = fetchSummariesFromCommonBlock(repository, peer, ourInitialHeight,
						force, peerBlockSummaries, true);
				if (commonResult != SynchronizationResult.OK) {
					peer.setCommonBlockData(null);
					return commonResult;
				}

				final BlockData commonBlockData = repository.getBlockRepository()
						.fromSignature(peerBlockSummaries.get(0).getSignature());
				if (commonBlockData == null)
					return SynchronizationResult.NO_COMMON_BLOCK;
				final int commonBlockHeight = commonBlockData.getHeight();
				LOGGER.debug(String.format("Common block with peer %s is at height %d, sig %.8s, ts %d", peer,
						commonBlockHeight, Base58.encode(commonBlockData.getSignature()), commonBlockData.getTimestamp()));
				peerBlockSummaries.remove(0);

				if (commonBlockHeight > peerHeight)
					peerHeight = commonBlockHeight;
				if (commonBlockHeight == peerHeight)
					return SynchronizationResult.NOTHING_TO_DO;

				if (!force && ourInitialHeight > commonBlockHeight) {
					SynchronizationResult comparison = compareChains(repository, commonBlockData, ourLatestBlockData,
							peer, peerHeight, peerBlockSummaries);
					if (comparison != SynchronizationResult.OK)
						return comparison;
				}

				final boolean reorg = commonBlockHeight < ourInitialHeight;
				if (!reorg)
					return synchronizeAppendSession(repository, blockchainLock, peer, ourLatestBlockData,
							peerHeight, commonBlockData, peerBlockSummaries);

				BlockFetchPlan plan = fetchReorgPlan(repository, peer, commonBlockData, peerHeight, peerBlockSummaries);
				if (plan.result != SynchronizationResult.OK)
					return plan.result;

				// End any read transaction used while preparing. The post-lock tip read must see
				// the latest committed chain, not the preparation snapshot.
				repository.discardChanges();
				if (!blockchainLock.tryLock(3, TimeUnit.SECONDS)) {
					LOGGER.info("Synchronizer couldn't acquire blockchain lock");
					return SynchronizationResult.NO_BLOCKCHAIN_LOCK;
				}

				final long lockHeldStartMillis = System.currentTimeMillis();
				try {
					BlockData currentTip = repository.getBlockRepository().getLastBlock();
					if (currentTip == null || !Arrays.equals(ourInitialSignature, currentTip.getSignature())) {
						LOGGER.debug("Blockchain tip changed while preparing synchronization with {}; rebuilding next round", peer);
						return SynchronizationResult.NOTHING_TO_DO;
					}

					SynchronizationResult applyResult = applyReorgPlan(repository, commonBlockData,
							ourInitialHeight, peer, plan.blocks);
					if (applyResult != SynchronizationResult.OK)
						return applyResult;

					repository.saveChanges();
					BlockData newLatestBlockData = repository.getBlockRepository().getLastBlock();
					String syncLog = String.format("Synchronized with peer %s to height %d, sig %.8s, ts: %d", peer,
							newLatestBlockData.getHeight(), Base58.encode(newLatestBlockData.getSignature()),
							newLatestBlockData.getTimestamp());
					if (this.lastReorgSize > 0)
						syncLog = syncLog.concat(String.format(", size: %d", this.lastReorgSize));
					LOGGER.info(syncLog);
					return SynchronizationResult.OK;
				} finally {
					long lockHeldMillis = System.currentTimeMillis() - lockHeldStartMillis;
					if (lockHeldMillis >= 2000L)
						LOGGER.info("Synchronizer held blockchain lock for {} ms with peer {}", lockHeldMillis, peer);
					blockchainLock.unlock();
				}
			} finally {
				repository.discardChanges();
			}
		} catch (DataException e) {
			LOGGER.error("Repository issue during synchronization with peer", e);
			return SynchronizationResult.REPOSITORY_ISSUE;
		}
	}

	/**
	 * Continue an append-only synchronization with the same peer for up to one sync session.
	 * Network fetches happen without the blockchain lock, while every fetched batch gets its
	 * own tip check and short lock hold before it is applied.
	 */
	private SynchronizationResult synchronizeAppendSession(Repository repository, ReentrantLock blockchainLock,
			Peer peer, BlockData initialTip, int peerHeight, BlockData commonBlockData,
			List<BlockSummaryData> peerBlockSummaries) throws InterruptedException, DataException {
		int maximumSessionHeight = Math.min(peerHeight, initialTip.getHeight() + SYNC_BATCH_SIZE);
		BlockData expectedTip = initialTip;
		List<BlockSummaryData> availableSummaries = peerBlockSummaries;
		boolean allowFastBatch = true;

		while (expectedTip.getHeight() < maximumSessionHeight) {
			BlockFetchPlan plan = fetchAppendPlan(repository, peer, commonBlockData, expectedTip.getHeight(),
					maximumSessionHeight, availableSummaries, allowFastBatch);
			// A transport failure can leave a contiguous append prefix worth validating and
			// committing. Other failures (including invalid data) must not apply a prefix.
			if (plan.result != SynchronizationResult.OK
					&& (plan.result != SynchronizationResult.NO_REPLY || plan.blocks.isEmpty()))
				return plan.result;
			if (plan.blocks.isEmpty())
				return SynchronizationResult.NO_REPLY;
			if (plan.fastBatchUnavailable)
				allowFastBatch = false;

			byte[] expectedTipSignature = expectedTip.getSignature();
			repository.discardChanges();
			if (!blockchainLock.tryLock(3, TimeUnit.SECONDS)) {
				LOGGER.info("Synchronizer couldn't acquire blockchain lock");
				return SynchronizationResult.NO_BLOCKCHAIN_LOCK;
			}

			long lockHeldStartMillis = System.currentTimeMillis();
			BlockData newTip;
			try {
				BlockData currentTip = repository.getBlockRepository().getLastBlock();
				if (currentTip == null || !Arrays.equals(expectedTipSignature, currentTip.getSignature())) {
					LOGGER.debug("Blockchain tip changed while preparing synchronization with {}; rebuilding next round", peer);
					return SynchronizationResult.NOTHING_TO_DO;
				}

				SynchronizationResult applyResult = applyAppendPlan(repository, peer, plan.blocks);
				if (applyResult != SynchronizationResult.OK)
					return applyResult;

				repository.saveChanges();
				newTip = repository.getBlockRepository().getLastBlock();
				if (newTip == null || newTip.getHeight() <= expectedTip.getHeight())
					return SynchronizationResult.REPOSITORY_ISSUE;
			} finally {
				long lockHeldMillis = System.currentTimeMillis() - lockHeldStartMillis;
				if (lockHeldMillis >= 2000L)
					LOGGER.info("Synchronizer held blockchain lock for {} ms with peer {}", lockHeldMillis, peer);
				blockchainLock.unlock();
			}

			expectedTip = newTip;
			commonBlockData = newTip;
			availableSummaries = Collections.emptyList();
			repository.discardChanges();
			if (plan.result == SynchronizationResult.NO_REPLY) {
				LOGGER.info("Applied {} blocks from peer {} through height {} before a request failed; retrying with peer selection",
						plan.blocks.size(), peer, newTip.getHeight());
				// Preserve the failed-request result so the caller can cool down this peer.
				return SynchronizationResult.NO_REPLY;
			}
		}

		LOGGER.info("Synchronized with peer {} to height {}, sig {}, ts: {}", peer, expectedTip.getHeight(),
				Base58.encode(expectedTip.getSignature()).substring(0, 8), expectedTip.getTimestamp());
		return SynchronizationResult.OK;
	}

	private static final class BlockFetchPlan {
		// NO_REPLY may carry a valid append prefix. Reorg plans never carry partial blocks.
		private final SynchronizationResult result;
		private final List<Block> blocks;
		private final boolean fastBatchUnavailable;

		private BlockFetchPlan(SynchronizationResult result, List<Block> blocks, boolean fastBatchUnavailable) {
			this.result = result;
			this.blocks = blocks;
			this.fastBatchUnavailable = fastBatchUnavailable;
		}

		private static BlockFetchPlan success(List<Block> blocks) {
			return new BlockFetchPlan(SynchronizationResult.OK, blocks, false);
		}

		private static BlockFetchPlan failure(SynchronizationResult result) {
			return new BlockFetchPlan(result, Collections.emptyList(), false);
		}
	}

	/** Fetch a complete replacement segment before any local block is orphaned. */
	private BlockFetchPlan fetchReorgPlan(Repository repository, Peer peer, BlockData commonBlockData,
			int peerHeight, List<BlockSummaryData> peerBlockSummaries) throws InterruptedException, DataException {
		byte[] latestSignature = commonBlockData.getSignature();
		int height = commonBlockData.getHeight();
		int maximumHeight = Math.min(peerHeight, height + MAXIMUM_COMMON_DELTA);
		List<byte[]> signatures = peerBlockSummaries.stream().map(BlockSummaryData::getSignature)
				.collect(Collectors.toCollection(ArrayList::new));
		List<Block> blocks = new ArrayList<>();
		int retryCount = 0;
		Settings settings = Settings.getInstance();
		int maxRetries = settings.getMaxRetries();
		int batchLimit = Math.min(settings.getMaxBlocksPerRequest(), Network.MAX_BLOCKS_PER_REPLY);
		Long peerVersion = peer.getPeersVersion();
		boolean useBatches = settings.isFastSyncEnabled() && settings.isFastSyncEnabledWhenResolvingFork()
				&& peerVersion != null && peerVersion >= PEER_VERSION_550 && batchLimit > 1;

		while (height < maximumHeight) {
			if (Controller.isStopping())
				return BlockFetchPlan.failure(SynchronizationResult.SHUTTING_DOWN);
			if (signatures.isEmpty()) {
				int requested = Math.min(maximumHeight - height, MAXIMUM_REQUEST_SIZE);
				signatures = this.getBlockSignatures(peer, latestSignature, requested);
				if (signatures == null || signatures.isEmpty())
					return BlockFetchPlan.failure(SynchronizationResult.NO_REPLY);
			}
			if (this.containsInvalidBlockSignature(signatures))
				return BlockFetchPlan.failure(SynchronizationResult.INVALID_DATA);

			int count = Math.min(signatures.size(), maximumHeight - height);
			List<Block> received;
			if (useBatches && count > 1) {
				int requested = Math.min(count, batchLimit);
				received = this.fetchBlocks(repository, peer, latestSignature, requested);
				if (received == null || received.isEmpty()) {
					useBatches = false;
					continue;
				}
				if (received.size() > requested)
					return BlockFetchPlan.failure(SynchronizationResult.INVALID_DATA);
			} else {
				byte[] requestedSignature = signatures.get(0);
				Block block = this.fetchBlock(repository, peer, requestedSignature);
				if (block == null) {
					if (retryCount++ >= maxRetries || peer.isStopping())
						return BlockFetchPlan.failure(SynchronizationResult.NO_REPLY);
					// Recheck the peer's fork before a retry, keeping the selected signatures pinned.
					List<byte[]> successors = this.getBlockSignatures(peer, latestSignature, 1);
					if (successors == null || successors.isEmpty())
						return BlockFetchPlan.failure(SynchronizationResult.NO_REPLY);
					if (!Arrays.equals(successors.get(0), requestedSignature))
						return BlockFetchPlan.failure(SynchronizationResult.NOTHING_TO_DO);
					continue;
				}
				received = Collections.singletonList(block);
			}

			for (int i = 0; i < received.size(); ++i) {
				if (Controller.isStopping())
					return BlockFetchPlan.failure(SynchronizationResult.SHUTTING_DOWN);
				Block block = received.get(i);
				if (block == null || !Objects.equals(block.getBlockData().getHeight(), height + 1)
						|| !Arrays.equals(block.getBlockData().getReference(), latestSignature)
						|| !Arrays.equals(block.getSignature(), signatures.get(i)))
					return BlockFetchPlan.failure(SynchronizationResult.INVALID_DATA);
				// Where a summary was used for fork choice, bind its weight inputs to the body too.
				if (blocks.size() < peerBlockSummaries.size()) {
					BlockSummaryData summary = peerBlockSummaries.get(blocks.size());
					if (summary.getHeight() != height + 1
							|| !Arrays.equals(summary.getMinterPublicKey(), block.getBlockData().getMinterPublicKey())
							|| !Objects.equals(summary.getOnlineAccountsCount(), block.getBlockData().getOnlineAccountsCount()))
						return BlockFetchPlan.failure(SynchronizationResult.INVALID_DATA);
				}
				// BLOCKS replies are decoded without a repository. Match the single-block
				// validation context before verifying transactions and setting approval status.
				block.setRepository(repository);
				if (!block.isSignatureValid())
					return BlockFetchPlan.failure(SynchronizationResult.INVALID_DATA);
				for (Transaction transaction : block.getTransactions())
					transaction.setInitialApprovalStatus();
				blocks.add(block);
				latestSignature = block.getSignature();
				++height;
			}
			signatures.subList(0, received.size()).clear();
			retryCount = 0;
		}
		BlockSummaryData currentPeerTip = peer.getChainTipData();
		if (currentPeerTip != null) {
			if (currentPeerTip.getHeight() == height && !Arrays.equals(currentPeerTip.getSignature(), latestSignature))
				return BlockFetchPlan.failure(SynchronizationResult.NOTHING_TO_DO);
			Long minimumTimestamp = Controller.getMinimumLatestBlockTimestamp();
			Long peerTimestamp = currentPeerTip.getTimestamp();
			if (minimumTimestamp != null && (peerTimestamp == null || peerTimestamp < minimumTimestamp))
				return BlockFetchPlan.failure(SynchronizationResult.CHAIN_TIP_TOO_OLD);
		}
		return BlockFetchPlan.success(blocks);
	}

	/** Fetch one bounded forward batch before taking the blockchain lock. */
	private BlockFetchPlan fetchAppendPlan(Repository repository, Peer peer, BlockData commonBlockData,
			int ourInitialHeight, int peerHeight, List<BlockSummaryData> peerBlockSummaries,
			boolean allowFastBatch) throws InterruptedException, DataException {
		int remaining = Math.min(peerHeight - ourInitialHeight, MAXIMUM_REQUEST_SIZE);
		if (remaining <= 0)
			return BlockFetchPlan.success(Collections.emptyList());

		if (allowFastBatch && Settings.getInstance().isFastSyncEnabled() && peer.getPeersVersion() >= PEER_VERSION_550) {
			int requested = Math.min(remaining, Settings.getInstance().getMaxBlocksPerRequest());
			List<Block> blocks = this.fetchBlocks(repository, peer, commonBlockData.getSignature(), requested);
			if (blocks == null || blocks.isEmpty()) {
				LOGGER.debug("Peer {} did not return a usable fast block batch after height {}; falling back to individual blocks",
						peer, ourInitialHeight);
				BlockFetchPlan fallback = fetchAppendPlanIndividually(repository, peer, commonBlockData,
						peerBlockSummaries, requested);
				return new BlockFetchPlan(fallback.result, fallback.blocks, true);
			}
			if (blocks.size() > requested)
				return BlockFetchPlan.failure(SynchronizationResult.INVALID_DATA);

			byte[] expectedReference = commonBlockData.getSignature();
			int expectedHeight = ourInitialHeight + 1;
			for (Block block : blocks) {
				if (block == null || block.getBlockData().getHeight() != expectedHeight
						|| !Arrays.equals(block.getBlockData().getReference(), expectedReference)
						|| !block.isSignatureValid())
					return BlockFetchPlan.failure(SynchronizationResult.INVALID_DATA);
				expectedReference = block.getSignature();
				++expectedHeight;
			}
			return BlockFetchPlan.success(blocks);
		}

		int requested = Math.min(remaining, Settings.getInstance().getMaxBlocksPerRequest());
		return fetchAppendPlanIndividually(repository, peer, commonBlockData, peerBlockSummaries, requested);
	}

	private BlockFetchPlan fetchAppendPlanIndividually(Repository repository, Peer peer, BlockData commonBlockData,
			List<BlockSummaryData> peerBlockSummaries, int numberRequested) throws InterruptedException, DataException {
		byte[] latestSignature = commonBlockData.getSignature();
		List<byte[]> signatures = peerBlockSummaries.stream().map(BlockSummaryData::getSignature)
				.collect(Collectors.toCollection(ArrayList::new));
		List<Block> blocks = new ArrayList<>();
		SynchronizationResult fetchResult = SynchronizationResult.OK;
		while (blocks.size() < numberRequested) {
			if (Controller.isStopping())
				return BlockFetchPlan.failure(SynchronizationResult.SHUTTING_DOWN);
			if (signatures.isEmpty()) {
				signatures = this.getBlockSignatures(peer, latestSignature, numberRequested - blocks.size());
				if (signatures == null || signatures.isEmpty()) {
					fetchResult = SynchronizationResult.NO_REPLY;
					break;
				}
			}
			if (this.containsInvalidBlockSignature(signatures))
				return BlockFetchPlan.failure(SynchronizationResult.INVALID_DATA);
			byte[] requestedSignature = signatures.remove(0);
			Block block = this.fetchBlock(repository, peer, requestedSignature);
			if (block == null) {
				fetchResult = SynchronizationResult.NO_REPLY;
				break;
			}
			if (!Objects.equals(block.getBlockData().getHeight(), commonBlockData.getHeight() + blocks.size() + 1)
					|| !Arrays.equals(latestSignature, block.getBlockData().getReference())
					|| !Arrays.equals(requestedSignature, block.getSignature()) || !block.isSignatureValid())
				return BlockFetchPlan.failure(SynchronizationResult.INVALID_DATA);
			for (Transaction transaction : block.getTransactions())
				transaction.setInitialApprovalStatus();
			blocks.add(block);
			latestSignature = requestedSignature;
		}
		if (peer.getChainTipData() != null) {
			Long minimumTimestamp = Controller.getMinimumLatestBlockTimestamp();
			Long peerTimestamp = peer.getChainTipData().getTimestamp();
			if (minimumTimestamp != null && (peerTimestamp == null || peerTimestamp < minimumTimestamp))
				return BlockFetchPlan.failure(SynchronizationResult.CHAIN_TIP_TOO_OLD);
		}
		return new BlockFetchPlan(fetchResult, blocks, false);
	}

	private SynchronizationResult applyReorgPlan(Repository repository, BlockData commonBlockData,
			int ourInitialHeight, Peer peer, List<Block> peerBlocks) throws DataException {
		int ourHeight = ourInitialHeight;
		BlockData orphanBlockData = repository.getBlockRepository().fromHeight(ourInitialHeight);
		while (ourHeight > commonBlockData.getHeight()) {
			if (Controller.isStopping())
				return SynchronizationResult.SHUTTING_DOWN;
			new Block(repository, orphanBlockData).orphan();
			repository.saveChanges();
			--ourHeight;
			orphanBlockData = repository.getBlockRepository().fromHeight(ourHeight);
			repository.discardChanges();
			Controller.getInstance().onOrphanedBlock(orphanBlockData);
		}

		for (Block block : peerBlocks) {
			if (Controller.isStopping())
				return SynchronizationResult.SHUTTING_DOWN;
			block.setRepository(repository);
			block.preProcess();
			ValidationResult validation = block.isValid();
			if (validation != ValidationResult.OK) {
				this.addInvalidBlockSignature(block.getSignature());
				this.timeInvalidBlockLastReceived = NTP.getTime();
				LOGGER.info("Peer {} sent invalid prepared block at height {}: {}", peer,
						block.getBlockData().getHeight(), validation);
				return SynchronizationResult.INVALID_DATA;
			}
			this.timeValidBlockLastReceived = NTP.getTime();
			for (Transaction transaction : block.getTransactions()) {
				TransactionData data = transaction.getTransactionData();
				if (data.getType() != Transaction.TransactionType.AT)
					repository.getTransactionRepository().save(data);
			}
			block.process();
			repository.saveChanges();
			synchronized (this.syncLock) {
				if (peer.getChainTipData() != null)
					this.blocksRemaining = peer.getChainTipData().getHeight() - block.getBlockData().getHeight();
			}
			Controller.getInstance().onNewBlock(block.getBlockData());
		}
		this.lastReorgSize = ourInitialHeight - commonBlockData.getHeight();
		return SynchronizationResult.OK;
	}

	private SynchronizationResult applyAppendPlan(Repository repository, Peer peer, List<Block> blocks) throws DataException {
		int uncommittedBlocks = 0;
		long atWorkspaceBytes = 0;
		for (int index = 0; index < blocks.size(); ++index) {
			Block block = blocks.get(index);
			if (Controller.isStopping())
				return SynchronizationResult.SHUTTING_DOWN;
			block.setRepository(repository);
			for (Transaction transaction : block.getTransactions())
				transaction.setInitialApprovalStatus();
			ValidationResult validation = block.isValid();
			if (validation != ValidationResult.OK) {
				LOGGER.info("Peer {} sent invalid prepared block at height {}: {}", peer,
						block.getBlockData().getHeight(), validation);
				return SynchronizationResult.INVALID_DATA;
			}
			for (Transaction transaction : block.getTransactions()) {
				TransactionData data = transaction.getTransactionData();
				if (data.getType() != Transaction.TransactionType.AT)
					repository.getTransactionRepository().save(data);
			}
			block.process();
			BlockData committedBlockData = block.getBlockData();
			repository.afterCommit(() -> Controller.getInstance().onNewBlock(committedBlockData));
			synchronized (this.syncLock) {
				if (peer.getChainTipData() != null)
					this.blocksRemaining = peer.getChainTipData().getHeight() - committedBlockData.getHeight();
			}
			atWorkspaceBytes += block.getATWorkspaceBytes();
			blocks.set(index, null);
			if (++uncommittedBlocks >= 64 || atWorkspaceBytes >= 16L * 1024 * 1024) {
				repository.saveChanges();
				uncommittedBlocks = 0;
				atWorkspaceBytes = 0;
			}
		}
		repository.saveChanges();
		return SynchronizationResult.OK;
	}

	/**
	 * Returns list of peer's block summaries starting with common block with peer.
	 * 
	 * @param peer
	 * @return block summaries, or empty list if no common block, or null if there was an issue
	 * @throws DataException
	 * @throws InterruptedException
	 */
	public SynchronizationResult fetchSummariesFromCommonBlock(Repository repository, Peer peer, int ourHeight, boolean force, List<BlockSummaryData> blockSummariesFromCommon, boolean infoLogWhenNotFound) throws DataException, InterruptedException {
		// Start by asking for a few recent block hashes as this will cover a majority of reorgs
		// Failing that, back off exponentially
		int step = INITIAL_BLOCK_STEP;

		int testHeight = Math.max(ourHeight - step, 1);
		BlockData testBlockData = null;

		List<BlockSummaryData> blockSummariesBatch = null;

		while (testHeight >= 1) {
			// Are we shutting down?
			if (Controller.isStopping())
				return SynchronizationResult.SHUTTING_DOWN;

			// Fetch our block signature at this height
			testBlockData = repository.getBlockRepository().fromHeight(testHeight);
			if (testBlockData == null) {
				// Not found? But we've locked the blockchain and height is below blockchain's tip!
				LOGGER.error("Failed to get block at height lower than blockchain tip during synchronization?");
				return SynchronizationResult.REPOSITORY_ISSUE;
			}

			// Ask for block signatures since test block's signature
			byte[] testSignature = testBlockData.getSignature();
			LOGGER.trace(String.format("Requesting %d summar%s after height %d", step, (step != 1 ? "ies": "y"), testHeight));
			blockSummariesBatch = this.getBlockSummaries(peer, testSignature, step);

			if (blockSummariesBatch == null) {
				if (infoLogWhenNotFound)
					LOGGER.info(String.format("Error while trying to find common block with peer %s", peer));
				else
					LOGGER.debug(String.format("Error while trying to find common block with peer %s", peer));
				
				// No response - give up this time
				return SynchronizationResult.NO_REPLY;
			}

			LOGGER.trace(String.format("Received %s summar%s", blockSummariesBatch.size(), (blockSummariesBatch.size() != 1 ? "ies" : "y")));

			// Older peers deliberately return no summaries for pruned archived blocks. Their
			// signature endpoint can still prove that they know our test block by returning
			// its successor, which is enough to continue with the normal block fetch path.
			if (blockSummariesBatch.isEmpty() && peer.getChainTipData() != null
					&& peer.getChainTipData().getHeight() > testHeight) {
				List<byte[]> successorSignatures = this.getBlockSignatures(peer, testSignature, 1);
				if (successorSignatures == null)
					return SynchronizationResult.NO_REPLY;
				if (!successorSignatures.isEmpty()) {
					LOGGER.debug("Peer {} confirmed archived common block at height {} using its successor signature",
							peer, testHeight);
					break;
				}
			}

			// Empty list means remote peer is unaware of test signature OR has no new blocks after test signature
			if (!blockSummariesBatch.isEmpty())
				// We have entries so we have found a common block
				break;

			// No blocks after genesis block?
			// We don't get called for a peer at genesis height so this means NO blocks in common
			if (testHeight == 1) {
				LOGGER.info(String.format("Failure to find common block with peer %s", peer));
				return SynchronizationResult.NO_COMMON_BLOCK;
			}

			// If common block is too far behind us then we're on massively different forks so give up.
			if (!force && testHeight < ourHeight - MAXIMUM_COMMON_DELTA) {
				LOGGER.info(String.format("Blockchain too divergent with peer %s", peer));
				peer.setLastTooDivergentTime(NTP.getTime());
				return SynchronizationResult.TOO_DIVERGENT;
			}

			step <<= 1;
			step = Math.min(step, MAXIMUM_BLOCK_STEP);

			testHeight = Math.max(testHeight - step, 1);
		}

		// Peer not considered too divergent
		peer.setLastTooDivergentTime(0L);

		// Prepend test block's summary as first block summary, as summaries returned are *after* test block
		BlockSummaryData testBlockSummary = new BlockSummaryData(testBlockData);
		blockSummariesFromCommon.add(0, testBlockSummary);
		blockSummariesFromCommon.addAll(blockSummariesBatch);

		// Trim summaries so that first summary is common block.
		// Currently we work forward from common block until we hit a block we don't have
		// TODO: rewrite as modified binary search!
		int i;
		for (i = 1; i < blockSummariesFromCommon.size(); ++i) {
			if (Controller.isStopping())
				return SynchronizationResult.SHUTTING_DOWN;

			if (!repository.getBlockRepository().exists(blockSummariesFromCommon.get(i).getSignature()))
				break;
		}

		// Note: index i - 1 isn't cleared: List.subList is fromIndex inclusive to toIndex exclusive
		blockSummariesFromCommon.subList(0, i - 1).clear();

		return SynchronizationResult.OK;
	}

	private SynchronizationResult compareChains(Repository repository, BlockData commonBlockData, BlockData ourLatestBlockData,
			Peer peer, int peerHeight, List<BlockSummaryData> peerBlockSummaries) throws DataException, InterruptedException {
		final int commonBlockHeight = commonBlockData.getHeight();
		final byte[] commonBlockSig = commonBlockData.getSignature();

		// If our latest block is very old, we're very behind and should ditch our fork.
		final Long minLatestBlockTimestamp = Controller.getMinimumLatestBlockTimestamp();
		if (minLatestBlockTimestamp == null)
			return SynchronizationResult.REPOSITORY_ISSUE;

		if (ourLatestBlockData.getTimestamp() < minLatestBlockTimestamp) {
			LOGGER.info(String.format("Ditching our chain after height %d", commonBlockHeight));
		} else {
			// Compare chain weights

			LOGGER.debug(String.format("Comparing chains from block %d with peer %s", commonBlockHeight + 1, peer));

			// Fetch remaining peer's block summaries (which we also use to fill signatures list)
			int peerBlockCount = peerHeight - commonBlockHeight;

			while (peerBlockSummaries.size() < peerBlockCount) {
				if (Controller.isStopping())
					return SynchronizationResult.SHUTTING_DOWN;

				int lastSummaryHeight = commonBlockHeight + peerBlockSummaries.size();
				byte[] previousSignature;
				if (peerBlockSummaries.isEmpty())
					previousSignature = commonBlockSig;
				else
					previousSignature = peerBlockSummaries.get(peerBlockSummaries.size() - 1).getSignature();

				List<BlockSummaryData> moreBlockSummaries = this.getBlockSummaries(peer, previousSignature, peerBlockCount - peerBlockSummaries.size());

				if (moreBlockSummaries == null || moreBlockSummaries.isEmpty()) {
					LOGGER.info(String.format("Peer %s failed to respond with block summaries after height %d, sig %.8s", peer,
							lastSummaryHeight, Base58.encode(previousSignature)));
					return SynchronizationResult.NO_REPLY;
				}

				// Check peer sent valid heights
				for (int i = 0; i < moreBlockSummaries.size(); ++i) {
					if (Controller.isStopping())
						return SynchronizationResult.SHUTTING_DOWN;

					++lastSummaryHeight;

					BlockSummaryData blockSummary = moreBlockSummaries.get(i);

					if (blockSummary.getHeight() != lastSummaryHeight) {
						LOGGER.info(String.format("Peer %s responded with invalid block summary for height %d, sig %.8s", peer,
								lastSummaryHeight, Base58.encode(blockSummary.getSignature())));
						return SynchronizationResult.NO_REPLY;
					}
				}

				peerBlockSummaries.addAll(moreBlockSummaries);
			}

			// Fetch our corresponding block summaries
			List<BlockSummaryData> ourBlockSummaries = repository.getBlockRepository().getBlockSummaries(commonBlockHeight + 1, ourLatestBlockData.getHeight());

			// Populate minter account levels for both lists of block summaries
			populateBlockSummariesMinterLevels(repository, ourBlockSummaries);
			populateBlockSummariesMinterLevels(repository, peerBlockSummaries);

			final int mutualHeight = commonBlockHeight + Math.min(ourBlockSummaries.size(), peerBlockSummaries.size());

			// Calculate cumulative chain weights of both blockchain subsets, from common block to highest mutual block.
			BigInteger ourChainWeight = Block.calcChainWeight(commonBlockHeight, commonBlockSig, ourBlockSummaries, mutualHeight);
			BigInteger peerChainWeight = Block.calcChainWeight(commonBlockHeight, commonBlockSig, peerBlockSummaries, mutualHeight);

			NumberFormat accurateFormatter = new DecimalFormat("0.################E0");
			LOGGER.debug(String.format("commonBlockHeight: %d, commonBlockSig: %.8s, ourBlockSummaries.size(): %d, peerBlockSummaries.size(): %d", commonBlockHeight, Base58.encode(commonBlockSig), ourBlockSummaries.size(), peerBlockSummaries.size()));
			LOGGER.debug(String.format("Our chain weight: %s, peer's chain weight: %s (higher is better)", accurateFormatter.format(ourChainWeight), accurateFormatter.format(peerChainWeight)));

			// If our blockchain has greater weight then don't synchronize with peer
			if (ourChainWeight.compareTo(peerChainWeight) >= 0) {
				LOGGER.debug(String.format("Not synchronizing with peer %s as we have better blockchain", peer));
				return SynchronizationResult.INFERIOR_CHAIN;
			}
		}

		return SynchronizationResult.OK;
	}


	private List<BlockSummaryData> getBlockSummaries(Peer peer, byte[] parentSignature, int numberRequested) throws InterruptedException {
		Message getBlockSummariesMessage = new GetBlockSummariesMessage(parentSignature, numberRequested);

		// Use shorter timeout for sync operations to avoid blocking transaction processing
		Message message = peer.getResponseWithTimeout(getBlockSummariesMessage, SYNC_RESPONSE_TIMEOUT);
		if (message == null)
			return null;

		if (message.getType() == MessageType.BLOCK_SUMMARIES) {
			BlockSummariesMessage blockSummariesMessage = (BlockSummariesMessage) message;
			return blockSummariesMessage.getBlockSummaries();
		}
		else if (message.getType() == MessageType.BLOCK_SUMMARIES_V2) {
			BlockSummariesV2Message blockSummariesMessage = (BlockSummariesV2Message) message;
			return blockSummariesMessage.getBlockSummaries();
		}

		return null;
	}

	private List<byte[]> getBlockSignatures(Peer peer, byte[] parentSignature, int numberRequested) throws InterruptedException {
		Message getSignaturesMessage = new GetSignaturesV2Message(parentSignature, numberRequested);

		// Use shorter timeout for sync operations to avoid blocking transaction processing
		Message message = peer.getResponseWithTimeout(getSignaturesMessage, SYNC_RESPONSE_TIMEOUT);
		if (message == null || message.getType() != MessageType.SIGNATURES)
			return null;

		SignaturesMessage signaturesMessage = (SignaturesMessage) message;

		return signaturesMessage.getSignatures();
	}

	private Block fetchBlock(Repository repository, Peer peer, byte[] signature) throws InterruptedException {
		Message getBlockMessage = new GetBlockMessage(signature);

		// Use shorter timeout for sync operations to avoid blocking transaction processing
		Message message = peer.getResponseWithTimeout(getBlockMessage, SYNC_RESPONSE_TIMEOUT);
		if (message == null) {
			peer.getPeerData().incrementFailedSyncCount();
			if (peer.getPeerData().getFailedSyncCount() >= MAX_CONSECUTIVE_FAILED_SYNC_ATTEMPTS) {
				// Several failed attempts, so mark peer as misbehaved
				LOGGER.info("Marking peer {} as misbehaved due to {} failed sync attempts", peer, peer.getPeerData().getFailedSyncCount());
				Network.getInstance().peerMisbehaved(peer);
			}
			return null;
		}

		// Reset failed sync count now that we have a block response
		// FUTURE: we could move this to the end of the sync process, but to reduce risk this can be done
		// at a later stage. For now we are only defending against serialization errors or no responses.
		peer.getPeerData().setFailedSyncCount(0);

		switch (message.getType()) {
			case BLOCK: {
				BlockMessage blockMessage = (BlockMessage) message;
				return new Block(repository, blockMessage.getBlockData(), blockMessage.getTransactions(), blockMessage.getAtStates());
			}

			case BLOCK_V2: {
				BlockV2Message blockMessage = (BlockV2Message) message;
				return new Block(repository, blockMessage.getBlockData(), blockMessage.getTransactions(), blockMessage.getAtStatesHash());
			}

			default:
				return null;
		}
	}

    private List<Block> fetchBlocks(Repository repository, Peer peer, byte[] parentSignature, int numberRequested) throws InterruptedException {
        LOGGER.trace("Building GetBlocksMessage with parentSignature: {}, numberRequested: {}", parentSignature, numberRequested);
        Message getBlocksMessage = new GetBlocksMessage(parentSignature, numberRequested);

        Message message = peer.getResponseWithTimeout(getBlocksMessage, FETCH_BLOCKS_TIMEOUT);
        if (message == null || message.getType() != MessageType.BLOCKS) {
            LOGGER.warn("Received a null BLOCKS payload from {}", peer);
            return null;
        }

        BlocksMessage blocksMessage = (BlocksMessage) message;
        if (blocksMessage == null || blocksMessage.getBlocks() == null) {
            return null;
        }

        return blocksMessage.getBlocks();
    }

	public void populateBlockSummariesMinterLevels(Repository repository, List<BlockSummaryData> blockSummaries) throws DataException {
		final int firstBlockHeight = blockSummaries.get(0).getHeight();

		for (int i = 0; i < blockSummaries.size(); ++i) {
			if (Controller.isStopping())
				return;

			BlockSummaryData blockSummary = blockSummaries.get(i);

			// Qortal: minter is always a reward-share, so find actual minter and get their effective minting level
			int minterLevel = Account.getRewardShareEffectiveMintingLevel(repository, blockSummary.getMinterPublicKey());
			if (minterLevel == 0) {
				// It looks like this block's minter's reward-share has been cancelled.
				// So search for REWARD_SHARE transactions since common block to find missing minter info
				List<byte[]> transactionSignatures = repository.getTransactionRepository().getSignaturesMatchingCriteria(Transaction.TransactionType.REWARD_SHARE, null, firstBlockHeight, null);

				for (byte[] transactionSignature : transactionSignatures) {
					RewardShareTransactionData transactionData = (RewardShareTransactionData) repository.getTransactionRepository().fromSignature(transactionSignature);

					if (transactionData != null && Arrays.equals(transactionData.getRewardSharePublicKey(), blockSummary.getMinterPublicKey())) {
						Account rewardShareMinter = new PublicKeyAccount(repository, transactionData.getMinterPublicKey());
						minterLevel = rewardShareMinter.getEffectiveMintingLevel();
						break;
					}
				}

				if (minterLevel == 0) {
					// We don't want to throw, or use zero, as this will kill Controller thread and make client unstable.
					// So we log this but use 1 instead
					LOGGER.debug(() -> String.format("Unexpected zero effective minter level for reward-share %s - using 1 instead!", Base58.encode(blockSummary.getMinterPublicKey())));
					minterLevel = 1;
				}
			}

			blockSummary.setMinterLevel(minterLevel);
		}
	}

}
