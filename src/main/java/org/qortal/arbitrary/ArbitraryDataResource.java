package org.qortal.arbitrary;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.qortal.arbitrary.ArbitraryDataFile.ResourceIdType;
import org.qortal.arbitrary.exception.DataNotPublishedException;
import org.qortal.arbitrary.metadata.ArbitraryDataTransactionMetadata;
import org.qortal.arbitrary.misc.Service;
import org.qortal.controller.arbitrary.ArbitraryDataBuildManager;
import org.qortal.controller.arbitrary.ArbitraryDataManager;
import org.qortal.controller.arbitrary.ArbitraryDataStorageManager;
import org.qortal.controller.arbitrary.ArbitraryResourceStateRegistry;
import org.qortal.data.arbitrary.ArbitraryResourceData;
import org.qortal.data.arbitrary.ArbitraryResourceStatus;
import org.qortal.data.transaction.ArbitraryTransactionData;
import org.qortal.data.transaction.ArbitraryTransactionData.DataType;
import org.qortal.repository.DataException;
import org.qortal.repository.Repository;
import org.qortal.repository.RepositoryManager;
import org.qortal.settings.Settings;
import org.qortal.utils.FilesystemUtils;
import org.qortal.utils.ListUtils;
import org.qortal.utils.NTP;
import org.qortal.utils.StringUtils;
import org.qortal.utils.ByteArray;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.qortal.data.arbitrary.ArbitraryResourceStatus.Status;

public class ArbitraryDataResource {

    private static final Logger LOGGER = LogManager.getLogger(ArbitraryDataResource.class);

    private static final int MAX_PERSISTED_STATUS_KEYS = 4_096;
    private static final ConcurrentHashMap<PersistedStatusKey, PersistedStatus> PERSISTED_STATUSES = new ConcurrentHashMap<>();
    private static final ConcurrentLinkedQueue<PersistedStatusKey> PERSISTED_STATUS_ORDER = new ConcurrentLinkedQueue<>();

    protected final String resourceId;
    protected final ResourceIdType resourceIdType;
    protected final Service service;
    protected final String identifier;

    private ArbitraryTransactionData transaction;
    private ArbitraryTransactionData latestPutTransaction;
    private ArbitraryTransactionData latestTransaction;

    private Integer localChunkCount = null;
    private Integer totalChunkCount = null;
    private boolean exists = false;


    public ArbitraryDataResource(String resourceId, ResourceIdType resourceIdType, Service service, String identifier) {
        this.resourceId = resourceId.toLowerCase();
        this.resourceIdType = resourceIdType;
        this.service = service;

        // If identifier is a blank string, or reserved keyword "default", treat it as null
        if (identifier == null || identifier.isEmpty() || identifier.equals("default")) {
            identifier = null;
        }
        this.identifier = identifier;
    }

    public ArbitraryResourceStatus getStatusAndUpdateCache(boolean updateCache) {
        try {
            ArbitraryResourceStateRegistry registry = ArbitraryResourceStateRegistry.getInstance();
            ArbitraryResourceStatus resourceStatus = registry.getOrReconcile(
                    this.service, this.resourceId, this.identifier, null, () -> {
                        try (final Repository repository = RepositoryManager.getRepository()) {
                            ArbitraryResourceStatus calculatedStatus = this.getStatus(repository);
                            if (updateCache) {
                                Status status = calculatedStatus != null ? calculatedStatus.getStatus() : null;
                                this.persistStatusIfChanged(repository, status);
                            }
                            return calculatedStatus;
                        }
                    });
            if (this.transaction != null && this.transaction.getSignature() != null)
                registry.associateSignature(this.service, this.resourceId, this.identifier,
                        this.transaction.getSignature());
            return resourceStatus;
        } catch (Exception e) {
            LOGGER.info("Unable to update status cache for resource {}: {}", this.toString(), e.getMessage());
            return null;
        }
    }

    private void persistStatusIfChanged(Repository repository, Status status) throws DataException {
        if (status == null || this.transaction == null || this.transaction.getSignature() == null)
            return;

        PersistedStatusKey lookupKey = new PersistedStatusKey(this.service, this.resourceId, this.identifier,
                this.transaction.getSignature(), false);
        PersistedStatus state = PERSISTED_STATUSES.get(lookupKey);
        if (state == null) {
            PersistedStatusKey storedKey = new PersistedStatusKey(this.service, this.resourceId, this.identifier,
                    this.transaction.getSignature(), true);
            PersistedStatus newState = new PersistedStatus();
            PersistedStatus existing = PERSISTED_STATUSES.putIfAbsent(storedKey, newState);
            state = existing != null ? existing : newState;
            if (existing == null) {
                PERSISTED_STATUS_ORDER.offer(storedKey);
                trimPersistedStatuses();
            }
        }

        synchronized (state) {
            if (state.status == status)
                return;

            ArbitraryResourceData resourceData = new ArbitraryResourceData(this.service, this.resourceId, this.identifier);
            resourceData.latestSignature = this.transaction.getSignature();

            // End the read transaction before the small independent cache update.
            repository.discardChanges();
            boolean changed = repository.getArbitraryRepository().setStatus(resourceData, status);
            repository.saveChanges();

            if (changed) {
                state.status = status;
                return;
            }

            // A zero-row conditional update can mean either "already current" or "cache row absent".
            // Only remember the result when the row exists with the requested value, so a resource
            // inserted by the dirty queue later will still receive its status on the next poll.
            ArbitraryResourceData stored = repository.getArbitraryRepository()
                    .getArbitraryResource(this.service, this.resourceId, this.identifier);
            if (stored != null && stored.status != null && stored.status.getStatus() == status)
                state.status = status;
        }
    }

    private static void trimPersistedStatuses() {
        while (PERSISTED_STATUSES.size() > MAX_PERSISTED_STATUS_KEYS) {
            PersistedStatusKey oldest = PERSISTED_STATUS_ORDER.poll();
            if (oldest == null)
                return;
            PERSISTED_STATUSES.remove(oldest);
        }
    }

    private static final class PersistedStatus {
        private Status status;
    }

    private static final class PersistedStatusKey {
        private final int service;
        private final String name;
        private final String identifier;
        private final ByteArray signature;
        private final int hashCode;

        private PersistedStatusKey(Service service, String name, String identifier, byte[] signature, boolean copy) {
            this.service = service.value;
            this.name = name.toLowerCase(Locale.ROOT);
            this.identifier = identifier != null ? identifier : "default";
            this.signature = copy ? ByteArray.copyOf(signature) : ByteArray.wrap(signature);
            this.hashCode = Objects.hash(this.service, this.name, this.identifier, this.signature);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other)
                return true;
            if (!(other instanceof PersistedStatusKey))
                return false;
            PersistedStatusKey key = (PersistedStatusKey) other;
            return this.service == key.service && this.name.equals(key.name)
                    && this.identifier.equals(key.identifier) && this.signature.equals(key.signature);
        }

        @Override
        public int hashCode() {
            return this.hashCode;
        }
    }

    /**
     * Get current status of resource
     *
     * @param repository
     * @return the resource's status
     */
    public ArbitraryResourceStatus getStatus(Repository repository) {
        // Calculate the chunk counts
        this.calculateChunkCounts(repository);

        if (!this.exists) {
            return new ArbitraryResourceStatus(Status.NOT_PUBLISHED, this.localChunkCount, this.totalChunkCount);
        }

        if (resourceIdType != ResourceIdType.NAME) {
            // We only support statuses for resources with a name
            return new ArbitraryResourceStatus(Status.UNSUPPORTED, this.localChunkCount, this.totalChunkCount);
        }

        // Check if the name is blocked
        if (ListUtils.isNameBlocked(this.resourceId)) {
            return new ArbitraryResourceStatus(Status.BLOCKED, this.localChunkCount, this.totalChunkCount);
        }

        // Check if a build has failed
        ArbitraryDataBuildQueueItem queueItem =
                new ArbitraryDataBuildQueueItem(resourceId, resourceIdType, service, identifier);
        if (ArbitraryDataBuildManager.getInstance().isInFailedBuildsList(queueItem)) {
            return new ArbitraryResourceStatus(Status.BUILD_FAILED, this.localChunkCount, this.totalChunkCount);
        }

        // Firstly check the cache to see if it's already built
        try {
            ArbitraryDataReader arbitraryDataReader = new ArbitraryDataReader(
                    resourceId, resourceIdType, service, identifier);
            if (arbitraryDataReader.isCachedDataAvailable()) {
                return new ArbitraryResourceStatus(Status.READY, this.localChunkCount, this.totalChunkCount);
            }
        } catch (DataException e) {
            // Assume no usable data
            return new ArbitraryResourceStatus(Status.PUBLISHED, this.localChunkCount, this.totalChunkCount);
        }

        // Check if we have all data locally for this resource
        if (!this.allFilesDownloaded(repository)) {
            if (this.isDownloading(repository)) {
                return new ArbitraryResourceStatus(Status.DOWNLOADING, this.localChunkCount, this.totalChunkCount);
            }
            else if (this.isDataPotentiallyAvailable(repository)) {
                return new ArbitraryResourceStatus(Status.PUBLISHED, this.localChunkCount, this.totalChunkCount);
            }
            return new ArbitraryResourceStatus(Status.MISSING_DATA, this.localChunkCount, this.totalChunkCount);
        }

        // Check if there's a build in progress
        if (ArbitraryDataBuildManager.getInstance().isInBuildQueue(queueItem)) {
            return new ArbitraryResourceStatus(Status.BUILDING, this.localChunkCount, this.totalChunkCount);
        }

        // We have all data locally
        return new ArbitraryResourceStatus(Status.DOWNLOADED, this.localChunkCount, this.totalChunkCount);
    }

    public ArbitraryDataTransactionMetadata getLatestTransactionMetadata() {
        this.fetchLatestTransaction();

        if (latestTransaction != null) {
            byte[] signature = latestTransaction.getSignature();
            byte[] metadataHash = latestTransaction.getMetadataHash();
            if (metadataHash == null) {
                // This resource doesn't have metadata
                return null;
            }

            try {
                ArbitraryDataFile metadataFile = ArbitraryDataFile.fromHash(metadataHash, signature);
                if (metadataFile.exists()) {
                    ArbitraryDataTransactionMetadata transactionMetadata = new ArbitraryDataTransactionMetadata(metadataFile.getFilePath());
                    transactionMetadata.read();
                    return transactionMetadata;
                }

            } catch (DataException | IOException e) {
                // Do nothing
            }
        }

        return null;
    }

    public boolean delete(Repository repository, boolean deleteMetadata) {
        try {
            this.fetchTransactions(repository);
            if (this.transaction == null) {
                return false;
            }

            ArbitraryDataFile arbitraryDataFile = ArbitraryDataFile.fromTransactionData(this.transaction);
            if (arbitraryDataFile != null) {

                // Delete any chunks or complete files from each transaction
                arbitraryDataFile.deleteAll(deleteMetadata);
            }

            // Also delete cached data for the entire resource
            this.deleteCache();

            // Invalidate the hosted transactions cache as we have removed an item
            ArbitraryDataStorageManager.getInstance().invalidateHostedTransactionsCache();

            return true;

        } catch (DataException | IOException e) {
            return false;
        }
    }

    public void deleteCache() throws IOException {
        // Don't delete anything if there's a build in progress
        ArbitraryDataBuildQueueItem queueItem =
                new ArbitraryDataBuildQueueItem(resourceId, resourceIdType, service, identifier);
        if (ArbitraryDataBuildManager.getInstance().isInBuildQueue(queueItem)) {
            return;
        }

        String baseDir = Settings.getInstance().getTempDataPath();
        String identifier = this.identifier != null ?  this.identifier : "default";
        Path cachePath = Paths.get(baseDir, "reader", this.resourceIdType.toString(), StringUtils.sanitizeString(this.resourceId), this.service.toString(), StringUtils.sanitizeString(identifier));
        if (cachePath.toFile().exists()) {
            boolean success = FilesystemUtils.safeDeleteDirectory(cachePath, true);
            if (success) {
                LOGGER.info("Cleared cache for resource {}", this.toString());
            }
        }
    }

    private boolean allFilesDownloaded(Repository repository) {
        // Use chunk counts to speed things up if we can
        if (this.localChunkCount != null && this.totalChunkCount != null &&
            this.localChunkCount >= this.totalChunkCount) {
            return true;
        }

        try {
            this.fetchTransactions(repository);
            if (this.transaction == null) {
                return false;
            }

            if (this.transaction.getDataType() == DataType.DATA_HASH) {
                Path dataPath = QdnPathResolver.resolve(
                        QdnPathResolver.encode(this.transaction.getData()), this.transaction.getSignature());
                if (Files.exists(dataPath))
                    return true;

                QdnFileManifestCache.Manifest manifest = QdnFileManifestCache.get(this.transaction);
                return manifest != null && manifest.allFilesExist();
            }

            // Optimized: Create ArbitraryDataFile once per transaction instead of twice
            ArbitraryDataFile arbitraryDataFile = ArbitraryDataFile.fromTransactionData(this.transaction);
            if (arbitraryDataFile == null || !arbitraryDataFile.allFilesExist()) {
                return false;
            }

            return true;
        } catch (DataException e) {
            return false;
        }
    }

  

    /**
     * Calculate chunk counts of a resource
     * Optimized to call fromTransactionData() only once per transaction
     *
     * @param repository optional - a new instance will be created if null
     */
    private void calculateChunkCounts(Repository repository) {
        try {
            this.fetchTransactions(repository);
            if (this.transaction == null) {
                this.exists = false;
                this.localChunkCount = 0;
                this.totalChunkCount = 0;
                return;
            }

            this.exists = true;

            int localChunkCount = 0;
            int totalChunkCount = 0;

            if (this.transaction.getDataType() == DataType.DATA_HASH) {
                Path dataPath = QdnPathResolver.resolve(
                        QdnPathResolver.encode(this.transaction.getData()), this.transaction.getSignature());
                QdnFileManifestCache.Manifest manifest = QdnFileManifestCache.get(this.transaction);
                int chunkCount = manifest != null ? manifest.chunkCount() : 0;

                Path parentPath = dataPath.getParent();
                String[] files = parentPath.toFile().list();
                if (files != null) {
                    int count = files.length;
                    for (String file : files) {
                        if (file.equals(".original")) {
                            count--;
                            break;
                        }
                    }

                    if (chunkCount > 0 && Files.exists(dataPath))
                        count--;
                    localChunkCount = count;
                }

                if (this.transaction.getMetadataHash() == null)
                    totalChunkCount = 1;
                else
                    totalChunkCount = manifest != null ? manifest.expectedFileCount() : 2;

                this.localChunkCount = localChunkCount;
                this.totalChunkCount = totalChunkCount;
                return;
            }

            // Create ArbitraryDataFile once and reuse it for both counts
            ArbitraryDataFile arbitraryDataFile = ArbitraryDataFile.fromTransactionData(this.transaction);
            if (arbitraryDataFile != null) {
                // Calculate local chunk count (files that exist on disk)
                Path parentPath = arbitraryDataFile.getFilePath().getParent();
                String[] files = parentPath.toFile().list();
                if (files != null) {
                    // Remove the original copy indicator file if it exists
                    int count = files.length;
                    for (String file : files) {
                        if (file.equals(".original")) {
                            count--;
                            break;
                        }
                    }

                    // If the complete file exists (and this transaction has chunks), subtract it from the count
                    if (arbitraryDataFile.chunkCount() > 0 && arbitraryDataFile.exists()) {
                        // We are only measuring the individual chunks, not the joined file
                        count -= 1;
                    }

                    localChunkCount += count;
                }

                // Calculate total chunk count (expected chunks based on metadata)
                if (this.transaction.getMetadataHash() == null) {
                    // This file doesn't have any metadata, therefore it has a single (complete) chunk
                    totalChunkCount += 1;
                } else {
                    totalChunkCount += arbitraryDataFile.fileCount();
                }
            }

            this.localChunkCount = localChunkCount;
            this.totalChunkCount = totalChunkCount;

        } catch (DataException e) {
            // If there's an error, set counts to 0
            this.localChunkCount = 0;
            this.totalChunkCount = 0;
        }
    }

    /**
     * Best guess as to whether data might be available
     * This is only used to give an indication to the user of progress
     * @return - whether data might be available on the network
     */
    private boolean isDataPotentiallyAvailable(Repository repository) {
        try {
            this.fetchTransactions(repository);
            if (this.transaction == null) {
                return false;
            }

            Long now = NTP.getTime();
            if (now == null) {
                return false;
            }

            long lastRequestTime = ArbitraryDataManager.getInstance().lastRequestForSignature(this.transaction.getSignature());
            // If we haven't requested yet, or requested in the last 30 seconds, there's still a
            // chance that data is on its way but hasn't arrived yet
            if (lastRequestTime == 0 || now - lastRequestTime < 30 * 1000L) {
                return true;
            }

            return false;

        } catch (DataException e) {
            return false;
        }
    }


    /**
     * Best guess as to whether we are currently downloading a resource
     * This is only used to give an indication to the user of progress
     * @return - whether we are trying to download the resource
     */
    private boolean isDownloading(Repository repository) {
        try {
            this.fetchTransactions(repository);
            if (this.transaction == null) {
                return false;
            }

            Long now = NTP.getTime();
            if (now == null) {
                return false;
            }

            long lastRequestTime = ArbitraryDataManager.getInstance().lastRequestForSignature(this.transaction.getSignature());
            // If were have requested data in the last 30 seconds, treat it as "downloading"
            if (lastRequestTime > 0 && now - lastRequestTime < 30 * 1000L) {
                return true;
            }

            // FUTURE: we may want to check for file hashes (including the metadata file hash) in
            // ArbitraryDataManager.arbitraryDataFileRequests and return true if one is found.

            return false;

        } catch (DataException e) {
            return false;
        }
    }


    /**
     * Fetch relevant arbitrary transactions for resource
     *
     * @param repository
     * @throws DataException
     */
    private void fetchTransactions(Repository repository) throws DataException {
        if (this.transaction != null ) {
            // Already fetched
            return;
        }

        try {
            byte[] latestSignature = repository.getArbitraryRepository().getLatestSignature(this.service, this.resourceId, this.identifier);

            if (latestSignature == null) {
                String message = String.format("Couldn't find PUT transaction for name %s, service %s and identifier %s",
                        this.resourceId, this.service, this.identifierString());
                throw new DataNotPublishedException(message);
            }

            ArbitraryTransactionData latestTransaction
                    = repository.getArbitraryRepository().getSingleTransactionBySignature(latestSignature);

            this.transaction = latestTransaction;

        } catch (DataNotPublishedException e) {
            // Ignore without logging
        }
        catch (DataException e) {
            LOGGER.info(String.format("Repository error when fetching transactions for resource %s: %s", this, e.getMessage()));
        }
    }

    private void fetchLatestTransaction() {
        if (this.latestTransaction != null) {
            // Already fetched
            return;
        }

        try (final Repository repository = RepositoryManager.getRepository()) {

            // Get the most recent transaction
            ArbitraryTransactionData latestTransaction = repository.getArbitraryRepository()
                    .getLatestTransaction(this.resourceId, this.service, null, this.identifier);
            if (latestTransaction == null) {
                String message = String.format("Couldn't find transaction for name %s, service %s and identifier %s",
                        this.resourceId, this.service, this.identifierString());
                throw new DataException(message);
            }
            this.latestTransaction = latestTransaction;

        } catch (DataException e) {
            LOGGER.info(String.format("Repository error when fetching latest transaction for resource %s: %s", this, e.getMessage()));
        }
    }

    private String resourceIdString() {
        return resourceId != null ? resourceId : "";
    }

    private String resourceIdTypeString() {
        return resourceIdType != null ? resourceIdType.toString() : "";
    }

    private String serviceString() {
        return service != null ? service.toString() : "";
    }

    private String identifierString() {
        return identifier != null ? identifier : "";
    }

    @Override
    public String toString() {
        return String.format("%s %s %s", this.serviceString(), this.resourceIdString(), this.identifierString());
    }


    /**
     * @return unique key used to identify this resource
     */
    public String getUniqueKey() {
        return String.format("%s-%s-%s", this.service, this.resourceId, this.identifier).toLowerCase();
    }

    public String getResourceId() {
        return this.resourceId;
    }

    public Service getService() {
        return this.service;
    }

    public String getIdentifier() {
        return this.identifier;
    }

    public Integer getLocalChunkCount() {
        return this.localChunkCount;
    }

    public Integer getTotalChunkCount() {
        return this.totalChunkCount;
    }


}
