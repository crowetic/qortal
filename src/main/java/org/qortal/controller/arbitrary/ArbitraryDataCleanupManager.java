package org.qortal.controller.arbitrary;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.qortal.arbitrary.ArbitraryDataFolderSizeEstimator;
import org.qortal.arbitrary.QdnFileManifestCache;
import org.qortal.arbitrary.QdnPathResolver;
import org.qortal.data.transaction.ArbitraryTransactionData;
import org.qortal.data.transaction.TransactionData;
import org.qortal.event.DataMonitorEvent;
import org.qortal.event.EventBus;
import org.qortal.repository.DataException;
import org.qortal.repository.Repository;
import org.qortal.repository.RepositoryManager;
import org.qortal.settings.Settings;
import org.qortal.transaction.Transaction;
import org.qortal.transaction.Transaction.TransactionType;
import org.qortal.utils.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.qortal.controller.arbitrary.ArbitraryDataStorageManager.DELETION_THRESHOLD;

public class ArbitraryDataCleanupManager extends Thread {

	private static final Logger LOGGER = LogManager.getLogger(ArbitraryDataCleanupManager.class);
	private static final List<TransactionType> ARBITRARY_TX_TYPE = Arrays.asList(TransactionType.ARBITRARY);
	private static final String QORTAL_RAW_DATA_PREFIX = "qortalRawData";
	private static final String QORTAL_TEMP_DIRECTORY_PREFIX = "qortal-";

	private static ArbitraryDataCleanupManager instance;

	private volatile boolean isStopping = false;

	/**
	 * The amount of time that must pass before a file is treated as stale / not recent.
	 * We can safely delete files created/accessed longer ago that this, if we have a means of
	 * rebuilding them. The main purpose of this is to avoid deleting files that are currently
	 * being used by other parts of the system.
	 */
	private static final long STALE_FILE_TIMEOUT = 60*60*1000L; // 1 hour

	/**
	 * The number of chunks to delete in a batch when over the capacity limit.
	 * Storage limits are re-checked after each batch, and there could be a significant
	 * delay between the processing of each batch as it only occurs after a complete
	 * cleanup cycle (to allow unwanted chunks to be deleted first).
	 *
	 * Above is the original comment. The batch size has been increased from 10 to 1000 and
	 * it occurs multiple times within a cleanup cycle now. The original way was not cleaning
	 * up data fast enough.
	 */
	private static final int CHUNK_DELETION_BATCH_SIZE = 1000;
	private static final long INDEX_AUDIT_INTERVAL = 5 * 60 * 1000L;
	private static final int INDEX_AUDIT_BATCH_SIZE = 25;
	private static final int DISCOVERY_BATCH_SIZE = 1_000;
	private static final long DISCOVERY_INTERVAL = 1_000L;
	private static final SecureRandom RANDOM = new SecureRandom();
	private long lastIndexedAudit;


	/*
	TODO:
	- Delete files from the _misc folder once they reach a certain age
	 */


	private ArbitraryDataCleanupManager() {
	}

	public static ArbitraryDataCleanupManager getInstance() {
		if (instance == null)
			instance = new ArbitraryDataCleanupManager();

		return instance;
	}

	@Override
	public void run() {
		Thread.currentThread().setName("Arbitrary Data Cleanup Manager");
		Thread.currentThread().setPriority(NORM_PRIORITY);

		final int indexedBatchSize = 100;
		List<Path> discoveryPaths = null;
		int discoveryOffset = 0;
		long lastSystemTempCleanup = 0L;
		long lastTempCleanup = 0L;
		LocalQdnContentJournal contentJournal = LocalQdnContentJournal.getInstance();

		try {
			while (!isStopping) {
				Thread.sleep(contentJournal.isDiscoveryComplete() ? 30_000L : DISCOVERY_INTERVAL);

				if (!Settings.getInstance().isQdnEnabled()) {
					Thread.sleep(60 * 60 * 1000L);
					continue;
				}

				Long now = NTP.getTime();
				if (now == null)
					continue;

				ArbitraryDataStorageManager storageManager = ArbitraryDataStorageManager.getInstance();
				if (now - lastSystemTempCleanup >= 30_000L) {
					cleanupSystemTempDirectory(now, STALE_FILE_TIMEOUT);
					lastSystemTempCleanup = now;
				}
				if (!storageManager.isStorageCapacityCalculated())
					continue;

				if (now - lastTempCleanup >= 5 * 60 * 1000L) {
					this.cleanupTempDirectory(now);
					lastTempCleanup = now;
				}

				if (!contentJournal.isDiscoveryComplete()) {
					if (discoveryPaths == null) {
						try {
							discoveryPaths = this.loadHostedPathsForDiscovery();
							discoveryOffset = 0;
							LOGGER.info("QDN local-content discovery found {} signature directories", discoveryPaths.size());
						} catch (IOException e) {
							LOGGER.warn("Unable to enumerate QDN content for discovery; retrying later", e);
							continue;
						}
					}

					discoveryOffset = this.processDiscoveryBatch(
							contentJournal, discoveryPaths, discoveryOffset, DISCOVERY_BATCH_SIZE);
					if (discoveryOffset >= discoveryPaths.size()) {
						contentJournal.markDiscoveryComplete();
						discoveryPaths = Collections.emptyList();
					}
					continue;
				}

				this.processIndexedBatch(contentJournal, storageManager, now, indexedBatchSize);
				if (!storageManager.isStorageSpaceAvailable(DELETION_THRESHOLD)) {
					LOGGER.info("no storage space available");
					Thread.sleep(10_000L);
					this.storageLimitReached();
				}
			}
		} catch (InterruptedException e) {
			// Fall through to exit.
		}
	}

	/**
	 * Enumerate only the actual signature-directory level. No transaction history is loaded,
	 * and no file contents or metadata are parsed during discovery.
	 */
	private List<Path> loadHostedPathsForDiscovery() throws IOException {
		Path dataPath = Paths.get(Settings.getInstance().getDataPath()).toAbsolutePath().normalize();
		if (!Files.isDirectory(dataPath))
			return new ArrayList<>(0);

		try (java.util.stream.Stream<Path> paths = Files.walk(dataPath, 3)) {
			return paths.filter(Files::isDirectory)
					.filter(path -> dataPath.relativize(path.toAbsolutePath().normalize()).getNameCount() == 3)
					.filter(path -> {
						Path relative = dataPath.relativize(path.toAbsolutePath().normalize());
						return !relative.getName(0).toString().startsWith("_");
					})
					.collect(Collectors.toList());
		}
	}

	private int processDiscoveryBatch(LocalQdnContentJournal contentJournal,
			List<Path> discoveryPaths, int offset, int limit) throws InterruptedException {
		int end = Math.min(discoveryPaths.size(), offset + limit);
		Map<String, byte[]> discovered = new java.util.LinkedHashMap<>();
		for (int i = offset; i < end; ++i) {
			if (isStopping)
				throw new InterruptedException();

			Path path = discoveryPaths.get(i);
			String signature58 = path.getFileName().toString();

			try {
				if (!this.directoryHasEntries(path) || !this.hasValidSignatureShard(path, signature58))
					continue;
				byte[] signature = Base58.decode(signature58);
				if (signature.length == 64 && !contentJournal.containsSignature(signature))
					discovered.put(signature58, signature);
			} catch (IOException | NumberFormatException e) {
				LOGGER.debug("Skipping invalid QDN discovery path {}", path);
			}
		}

		int added = contentJournal.confirmPresentBatch(discovered);
		if (added > 0)
			LOGGER.debug("QDN discovery persisted {} hosted signatures ({}/{})",
					added, end, discoveryPaths.size());
		return end;
	}

	private boolean directoryHasEntries(Path directory) throws IOException {
		try (java.nio.file.DirectoryStream<Path> entries = Files.newDirectoryStream(directory)) {
			return entries.iterator().hasNext();
		}
	}

	private boolean hasValidSignatureShard(Path path, String signature58) {
		if (signature58 == null || signature58.length() < 4 || path.getParent() == null
				|| path.getParent().getParent() == null)
			return false;
		String first = path.getParent().getParent().getFileName().toString();
		String second = path.getParent().getFileName().toString();
		return first.equalsIgnoreCase(signature58.substring(0, 2))
				&& second.equalsIgnoreCase(signature58.substring(2, 4));
	}

	private void processIndexedBatch(LocalQdnContentJournal contentJournal,
			ArbitraryDataStorageManager storageManager, long now, int limit) throws InterruptedException {
		List<byte[]> signatures = contentJournal.nextCandidates(limit, false);
		if (signatures.isEmpty() && now - this.lastIndexedAudit >= INDEX_AUDIT_INTERVAL) {
			signatures = contentJournal.nextCandidates(INDEX_AUDIT_BATCH_SIZE, true);
			this.lastIndexedAudit = now;
		}
		if (signatures.isEmpty())
			return;

		if (ArbitraryDataBuildManager.getInstance().getBuildInProgress()) {
			signatures.forEach(contentJournal::recordFilesystemChange);
			return;
		}

		Map<ByteArray, ArbitraryTransactionData> transactionBySignature = new HashMap<>();
		Map<ByteArray, ArbitraryTransactionData> newerPutBySignature = new HashMap<>();
		try (Repository repository = RepositoryManager.getRepository()) {
			List<TransactionData> transactionData = repository.getTransactionRepository().fromSignatures(signatures);
			for (TransactionData data : transactionData) {
				if (!(data instanceof ArbitraryTransactionData))
					continue;
				ArbitraryTransactionData arbitraryData = (ArbitraryTransactionData) data;
				ByteArray signatureKey = ByteArray.wrap(arbitraryData.getSignature());
				transactionBySignature.put(signatureKey, arbitraryData);
				if (arbitraryData.getName() != null && arbitraryData.getService() != null) {
					ArbitraryTransactionData latestPut = repository.getArbitraryRepository().getLatestTransaction(
							arbitraryData.getName(), arbitraryData.getService(),
							ArbitraryTransactionData.Method.PUT, arbitraryData.getIdentifier());
					if (latestPut != null && latestPut.getTimestamp() > arbitraryData.getTimestamp())
						newerPutBySignature.put(signatureKey, latestPut);
				}
			}
			repository.discardChanges();
		} catch (DataException e) {
			signatures.forEach(contentJournal::recordFilesystemChange);
			LOGGER.warn("Unable to resolve local QDN cleanup batch; deferring it", e);
			return;
		}

		for (byte[] signature : signatures) {
			if (isStopping)
				return;
			ByteArray signatureKey = ByteArray.wrap(signature);
			ArbitraryTransactionData transactionData = transactionBySignature.get(signatureKey);
			if (transactionData == null) {
				// Existing files are retained when the transaction cannot be resolved. Confirming
				// their presence removes this item from the dirty queue; the rotating audit will
				// check it again later without creating a permanent hot loop.
				try {
					if (this.indexedDirectoryHasEntries(signature))
						contentJournal.confirmPresent(signature);
					else
						contentJournal.confirmAbsent(signature);
				} catch (IOException e) {
					contentJournal.recordFilesystemChange(signature);
				}
				continue;
			}

			try {
				this.processIndexedTransaction(transactionData, newerPutBySignature.get(signatureKey),
						storageManager, contentJournal, now);
			} catch (DataException e) {
				contentJournal.recordFilesystemChange(signature);
				LOGGER.debug("Unable to reconcile local QDN data for {}; deferring it",
						QdnPathResolver.encode(signature), e);
			}
		}
	}

	private boolean indexedDirectoryHasEntries(byte[] signature) throws IOException {
		Path directory = QdnPathResolver.resolveTransactionDirectory(signature);
		if (directory == null)
			return false;
		if (!Files.isDirectory(directory))
			return false;
		try (java.util.stream.Stream<Path> paths = Files.list(directory)) {
			return paths.findAny().isPresent();
		}
	}

	private void processIndexedTransaction(ArbitraryTransactionData transactionData,
			ArbitraryTransactionData newerPut, ArbitraryDataStorageManager storageManager,
			LocalQdnContentJournal contentJournal, long now) throws DataException {
		LocalQdnContentJournal.SignatureLease cleanupLease =
				contentJournal.tryAcquireCleanupLease(transactionData.getSignature());
		if (cleanupLease == null) {
			contentJournal.recordFilesystemChange(transactionData.getSignature());
			return;
		}
		try (LocalQdnContentJournal.SignatureLease ignored = cleanupLease) {
		if (transactionData.getService() == null
				|| transactionData.getDataType() == ArbitraryTransactionData.DataType.RAW_DATA) {
			contentJournal.confirmAbsent(transactionData.getSignature());
			return;
		}

		QdnFileManifestCache.Manifest manifest = QdnFileManifestCache.get(transactionData);
		boolean completeFileExists = manifest != null
				? manifest.completeFileExists() : ArbitraryTransactionUtils.completeFileExists(transactionData);
		boolean anyChunksExist = manifest != null
				? manifest.anyChunksExist() : ArbitraryTransactionUtils.anyChunksExist(transactionData);
		if (!completeFileExists && !anyChunksExist) {
			contentJournal.confirmAbsent(transactionData.getSignature());
			return;
		}
		contentJournal.confirmPresent(transactionData.getSignature());

		if (!storageManager.canStoreData(transactionData)) {
			ArbitraryTransactionUtils.deleteCompleteFileAndChunks(transactionData);
			this.reconcileJournalEntry(contentJournal, transactionData);
			this.notifyCleanup(transactionData, "can't store data, deleting", transactionData.getTimestamp());
			return;
		}

		if (newerPut != null) {
			ArbitraryTransactionUtils.deleteCompleteFileAndChunks(transactionData);
			this.reconcileJournalEntry(contentJournal, transactionData);
			this.notifyCleanup(transactionData, "deleting data due to replacement", newerPut.getTimestamp());
			return;
		}

		boolean transactionHasChunks = transactionData.getMetadataHash() != null;
		if (completeFileExists && !transactionHasChunks)
			return;

		boolean allChunksExist = manifest != null
				? manifest.allChunksExist() : ArbitraryTransactionUtils.allChunksExist(transactionData);
		if (completeFileExists && allChunksExist) {
			if (ArbitraryTransactionUtils.deleteCompleteFile(transactionData, now, STALE_FILE_TIMEOUT)) {
				this.reconcileJournalEntry(contentJournal, transactionData);
				this.notifyCleanup(transactionData, "deleting file, retaining chunks", transactionData.getTimestamp());
			}
			return;
		}

		if (completeFileExists) {
			ArbitraryTransactionUtils.convertFileToChunks(transactionData, now, STALE_FILE_TIMEOUT);
			this.reconcileJournalEntry(contentJournal, transactionData);
		}
		}
	}

	private void reconcileJournalEntry(LocalQdnContentJournal contentJournal,
			ArbitraryTransactionData transactionData) throws DataException {
		QdnFileManifestCache.Manifest manifest = QdnFileManifestCache.get(transactionData);
		boolean contentExists = manifest != null
				? manifest.completeFileExists() || manifest.anyChunksExist()
				: ArbitraryTransactionUtils.completeFileExists(transactionData)
						|| ArbitraryTransactionUtils.anyChunksExist(transactionData);
		if (contentExists)
			contentJournal.confirmPresent(transactionData.getSignature());
		else
			contentJournal.confirmAbsent(transactionData.getSignature());
	}

	private void notifyCleanup(ArbitraryTransactionData transactionData, String reason, long latestTimestamp) {
		EventBus.INSTANCE.notify(new DataMonitorEvent(
				System.currentTimeMillis(), transactionData.getIdentifier(), transactionData.getName(),
				transactionData.getService().name(), reason, transactionData.getTimestamp(), latestTimestamp));
	}

	public List<Path> findPathsWithNoAssociatedTransaction(Repository repository) {
		List<Path> pathList = new ArrayList<>();

		// Find all hosted paths
		List<Path> allPaths = ArbitraryDataStorageManager.getInstance().findAllHostedPaths();

		// Loop through each path and find those without matching signatures
		for (Path path : allPaths) {
			if (isStopping) {
				break;
			}
			try {
				String[] contents = path.toFile().list();
				if (contents == null || contents.length == 0) {
					// Ignore empty directories
					continue;
				}

				String signature58 = path.getFileName().toString();
				byte[] signature = Base58.decode(signature58);
				TransactionData transactionData = repository.getTransactionRepository().fromSignature(signature);
				if (transactionData == null) {
					// No transaction data, and no DataException, so we can assume that this data relates to an expired transaction
					pathList.add(path);
				}

			} catch (DataException e) {
            }
		}

		return pathList;
	}

	private void checkForExpiredTransactions(Repository repository) {
		List<Path> expiredPaths = this.findPathsWithNoAssociatedTransaction(repository);
		for (Path expiredPath : expiredPaths) {
			if (isStopping) {
				return;
			}
			LOGGER.info("Found path with no associated transaction: {}", expiredPath.toString());
			this.safeDeleteDirectory(expiredPath.toFile(), "no matching transaction");
		}
	}

	private void storageLimitReached() throws InterruptedException {
		// We think that the storage limit has been reached

		LOGGER.info("no storage space available, proceed to delete");
		LocalQdnContentJournal contentJournal = LocalQdnContentJournal.getInstance();
		if (contentJournal.isDiscoveryComplete()) {
			if (ArbitraryDataBuildManager.getInstance().getBuildInProgress())
				return;
			List<byte[]> signatures = contentJournal.snapshotSignatures();
			Collections.shuffle(signatures, RANDOM);
			Map<ByteArray, ArbitraryTransactionData> transactionsBySignature = new HashMap<>();
			try (Repository repository = RepositoryManager.getRepository()) {
				for (TransactionData data : repository.getTransactionRepository().fromSignatures(signatures)) {
					if (data instanceof ArbitraryTransactionData) {
						ArbitraryTransactionData arbitraryData = (ArbitraryTransactionData) data;
						transactionsBySignature.put(ByteArray.wrap(arbitraryData.getSignature()), arbitraryData);
					}
				}
				repository.discardChanges();
			} catch (DataException e) {
				LOGGER.warn("Unable to resolve indexed QDN data for storage-pressure cleanup; deferring eviction", e);
				return;
			}
			int deleted = 0;
			long now = System.currentTimeMillis();
			for (byte[] signature : signatures) {
				if (isStopping || deleted >= CHUNK_DELETION_BATCH_SIZE)
					break;
				ArbitraryTransactionData transactionData =
						transactionsBySignature.get(ByteArray.wrap(signature));
				if (transactionData != null && this.deleteOneIndexedFile(transactionData, now))
					deleted++;
			}
			LOGGER.info("Deleted {} indexed QDN files after reaching the storage limit", deleted);
			return;
		}

		// Delete a batch of random chunks
		// This reduces the chance of too many nodes deleting the same chunk
		// when they reach their storage limit
		Path dataPath = Paths.get(Settings.getInstance().getDataPath());
		for (int i=0; i<CHUNK_DELETION_BATCH_SIZE; i++) {
			if (isStopping) {
				return;
			}
			this.deleteRandomFile(dataPath.toFile());
		}

		// FUTURE: consider reducing the expiry time of the reader cache
	}

	private boolean deleteOneIndexedFile(ArbitraryTransactionData transactionData, long now) {
		byte[] signature = transactionData.getSignature();
		LocalQdnContentJournal.SignatureLease cleanupLease =
				LocalQdnContentJournal.getInstance().tryAcquireCleanupLease(signature);
		if (cleanupLease == null) {
			LocalQdnContentJournal.getInstance().recordFilesystemChange(signature);
			return false;
		}
		try (LocalQdnContentJournal.SignatureLease ignored = cleanupLease) {
		Path directory = QdnPathResolver.resolveTransactionDirectory(signature);
		if (directory == null)
			return false;
		if (Files.exists(directory.resolve(".original")))
			return false;

		String metadataHash58 = transactionData.getMetadataHash() == null
				? null : Base58.encode(transactionData.getMetadataHash());
		File[] files = directory.toFile().listFiles(file -> file.isFile()
				&& !file.getName().equals(".original")
				&& !file.getName().equals(metadataHash58)
				&& !FilesystemUtils.isFileRecent(file.toPath(), now, STALE_FILE_TIMEOUT));
		if (files == null || files.length == 0) {
			LocalQdnContentJournal.getInstance().recordFilesystemChange(signature);
			return false;
		}

		File selected = files[RANDOM.nextInt(files.length)];
		try {
			LocalQdnContentJournal.getInstance().recordFilesystemChange(signature);
			long diskUsage = FilesystemUtils.getDiskUsage(selected);
			if (!selected.delete())
				return false;
			ArbitraryDataFolderSizeEstimator.getInstance().subtract(diskUsage);
			FilesystemUtils.safeDeleteEmptyParentDirectories(selected.toPath().getParent());
			return true;
		} catch (IOException e) {
			LOGGER.debug("Unable to delete indexed QDN file {}", selected, e);
			return false;
		}
		}
	}

	/**
	 * Iteratively walk through given directory and delete a single random file
	 *
	 * TODO: public data should be prioritized over private data
	 * (unless this node is part of a data market contract for that data).
	 * See: Service.privateServices() for a list of services containing private data.
	 *
	 * @param directory - the base directory
	 * @return boolean - whether a file was deleted
	 */
	private boolean deleteRandomFile(File directory) {
		Path tempDataPath = Paths.get(Settings.getInstance().getTempDataPath());

		// Pick a random directory
		final File[] contentsList = directory.listFiles();
		if (contentsList != null) {
			SecureRandom random = new SecureRandom();

			// If the directory is empty, there's nothing to do
			if (contentsList.length == 0) {
				return false;
			}

			File randomItem = contentsList[random.nextInt(contentsList.length)];
			if (LocalQdnContentJournal.getInstance().ownsPath(randomItem.toPath())) {
				return false;
			}

			// Skip anything relating to the temp directory
			if (FilesystemUtils.isChild(randomItem.toPath(), tempDataPath)) {
				return false;
			}
			// Make sure it exists
			if (!randomItem.exists()) {
				return false;
			}

			// If it's a directory, iteratively repeat the process
			if (randomItem.isDirectory()) {
				return this.deleteRandomFile(randomItem);
			}

			// If it's a file, we might be able to delete it
			if (randomItem.isFile()) {

				// If the parent directory contains an ".original" file, don't delete anything
				// This indicates that the content was originally updated by this node and so
				// could be the only copy that exists.
				Path originalCopyIndicatorPath = Paths.get(randomItem.getParent(), ".original");
				if (Files.exists(originalCopyIndicatorPath)) {
					// This is an original seed copy and so shouldn't be deleted
					return false;
				}

				LOGGER.debug("Deleting random file {} because we have reached max storage capacity...", randomItem.toString());

				long diskUsage = FilesystemUtils.getDiskUsage(randomItem);

				LocalQdnContentJournal.getInstance().recordPathChange(randomItem.toPath());
				boolean success = randomItem.delete();
				if (success) {

					ArbitraryDataFolderSizeEstimator.getInstance().subtract(diskUsage);
					LocalQdnContentJournal.getInstance().recordPathChange(randomItem.toPath());
					try {
						FilesystemUtils.safeDeleteEmptyParentDirectories(randomItem.toPath().getParent());
					} catch (IOException e) {
						// Ignore cleanup failure
					}
				}
				return success;
			}
		}
		return false;
	}

	private void fireRandomItemDeletionNotification(File randomItem, Repository repository, String reason) {
		try {
			Path parentFileNamePath = randomItem.toPath().toAbsolutePath().getParent().getFileName();
			if (parentFileNamePath != null) {
				String signature58 = parentFileNamePath.toString();
				byte[] signature = Base58.decode(signature58);
				TransactionData transactionData = repository.getTransactionRepository().fromSignature(signature);
				if (transactionData != null && transactionData.getType() == Transaction.TransactionType.ARBITRARY) {
					ArbitraryTransactionData arbitraryTransactionData = (ArbitraryTransactionData) transactionData;

					EventBus.INSTANCE.notify(
						new DataMonitorEvent(
							System.currentTimeMillis(),
							arbitraryTransactionData.getIdentifier(),
							arbitraryTransactionData.getName(),
							arbitraryTransactionData.getService().name(),
							reason,
							arbitraryTransactionData.getTimestamp(),
							arbitraryTransactionData.getTimestamp()
						)
					);
				}
			}

		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	private void cleanupTempDirectory(String folder, long now, long minAge) {
		String baseDir = Settings.getInstance().getTempDataPath();
		Path tempDir = Paths.get(baseDir, folder);
		int contentsCount = 0;

		// Loop through the contents and check each one
		final File[] directories = tempDir.toFile().listFiles();
		if (directories != null) {
			for (final File directory : directories) {
				if (isStopping) {
					return;
				}
				contentsCount++;

				// We're expecting the contents of each subfolder to be a directory
				if (directory.isDirectory()) {
					if (!FilesystemUtils.isFileRecent(directory.toPath(), now, minAge)) {
						// File isn't recent, so can be deleted
						this.safeDeleteDirectory(directory, "not recent");
					}
				}
			}
		}

		// If the directory is empty, we still need to delete its parent folder
		if (contentsCount == 0 && tempDir.toFile().isDirectory() && tempDir.toFile().exists()) {
			try {
				LOGGER.debug("Parent directory {} is empty, so deleting it", tempDir);
				FilesystemUtils.safeDeleteDirectory(tempDir, false);
			} catch(IOException e){
				LOGGER.info("Unable to delete parent directory: {}", tempDir);
			}
		}
	}

	/**
	 * Cleanup System Temp Directory
	 *
	 * Delete all files and directories that the Qortal Core may of stored in the system temp directory.
	 *
	 * @param now the timestamp for now
	 * @param minAge the amount of time to wait for deletion
	 */
	public void cleanupSystemTempDirectory(long now, long minAge) {

		// delete temp system files from Qortal, too
		Path systemTmpDirectory = Paths.get(System.getProperty("java.io.tmpdir"));

		try {
			ArbitraryTransactionUtils.deleteFilesByPrefix(systemTmpDirectory, QORTAL_RAW_DATA_PREFIX, now, minAge);
			ArbitraryTransactionUtils.deleteFoldersByPrefix(systemTmpDirectory, QORTAL_TEMP_DIRECTORY_PREFIX, now, minAge);
		} catch (IOException e) {
			LOGGER.warn("Unable to delete temp files: {}", systemTmpDirectory);
		}
	}

	private void cleanupReaderCache(Long now) {
		ArbitraryDataStorageManager storageManager = ArbitraryDataStorageManager.getInstance();
		String baseDir = Settings.getInstance().getTempDataPath();
		Path readerCachePath = Paths.get(baseDir, "reader");

		// Clean up names
		Path readerCacheNamesPath = Paths.get(readerCachePath.toString(), "NAME");

		// Loop through the contents and check each one
		final File[] directories = readerCacheNamesPath.toFile().listFiles();
		if (directories != null) {
			for (final File directory : directories) {
				if (isStopping) {
					return;
				}

				// Delete data relating to blocked names
				String name = directory.getName();
				if (name != null && ListUtils.isNameBlocked(name)) {
					this.safeDeleteDirectory(directory, "blocked name");
				}

				// Delete cached reader data that has reached its expiry
				this.cleanupReaderCacheForName(name, now);
			}
		}
	}

	private void cleanupReaderCacheForName(String name, Long now) {
		if (name == null) {
			return;
		}

		String baseDir = Settings.getInstance().getTempDataPath();
		Path readerNameCachePath = Paths.get(baseDir, "reader", "NAME", StringUtils.sanitizeString(name));

		// Loop through the contents and check each one
		final File[] directories = readerNameCachePath.toFile().listFiles();
		if (directories != null) {
			for (final File directory : directories) {
				if (isStopping) {
					return;
				}
				// Each directory is a "service" type
				String service = directory.getName();
				this.cleanupReaderCacheForNameAndService(name, service, now);
			}
		}
	}

	private void cleanupReaderCacheForNameAndService(String name, String service, Long now) {
		if (name == null || service == null) {
			return;
		}

		Path readerNameServiceCachePath = Paths.get("reader", "NAME", StringUtils.sanitizeString(name), service);
		Long expiry = Settings.getInstance().getBuiltDataExpiryInterval();
		this.cleanupTempDirectory(readerNameServiceCachePath.toString(), now, expiry);
	}

	private void cleanupTempDirectory(long now) {

		// Use the "stale file timeout" for the intermediate directories.
		// These aren't used for serving content - only for building it.
		// Once the files have become stale, it's safe to delete them.
		this.cleanupTempDirectory("diff",  now, STALE_FILE_TIMEOUT);
		this.cleanupTempDirectory("join",  now, STALE_FILE_TIMEOUT);
		this.cleanupTempDirectory("merge",  now, STALE_FILE_TIMEOUT);
		this.cleanupTempDirectory("writer",  now, STALE_FILE_TIMEOUT);

		// Built resources are served out of the "reader" directory so these
		// need to be kept around for much longer.
		// Purging currently disabled, as it's not very helpful. Will revisit
		// once we implement local storage limits.
		this.cleanupReaderCache(now);

	}

	private boolean safeDeleteDirectory(File directory, String reason) {
		LOGGER.debug("Deleting directory {} due to reason: {}", directory, reason);
		try {
			LocalQdnContentJournal.getInstance().recordPathChange(directory.toPath());
			FilesystemUtils.safeDeleteDirectory(directory.toPath(), true);
			LocalQdnContentJournal.getInstance().recordPathChange(directory.toPath());
			return true;
		} catch (IOException e) {
			LOGGER.debug("Unable to delete directory: {}", directory);
		}
		return false;
	}


	public void shutdown() {
		isStopping = true;
		this.interrupt();
		LocalQdnContentJournal.getInstance().compact();
	}

}
