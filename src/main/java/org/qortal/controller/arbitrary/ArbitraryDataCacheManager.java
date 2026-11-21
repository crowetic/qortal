package org.qortal.controller.arbitrary;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.qortal.controller.Controller;
import org.qortal.controller.Synchronizer;
import org.qortal.data.arbitrary.ArbitraryResourceData;
import org.qortal.data.arbitrary.ArbitraryResourceDirtyData;
import org.qortal.data.transaction.ArbitraryTransactionData;
import org.qortal.event.DataMonitorEvent;
import org.qortal.event.EventBus;
import org.qortal.gui.SplashFrame;
import org.qortal.repository.DataException;
import org.qortal.repository.Repository;
import org.qortal.repository.RepositoryManager;
import org.qortal.repository.hsqldb.HSQLDBDatabaseUpdates;
import org.qortal.settings.Settings;
import org.qortal.transaction.ArbitraryTransaction;
import org.qortal.utils.Base58;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.Set;
import java.util.stream.Collectors;

public class ArbitraryDataCacheManager extends Thread {

    private static final Logger LOGGER = LogManager.getLogger(ArbitraryDataCacheManager.class);

    private static ArbitraryDataCacheManager instance;
    private volatile boolean isStopping = false;

    private static final int UPDATE_BATCH_SIZE = 100;
    private static final long MAX_RETRY_DELAY = 3_600_000L;
    private static final long QUEUE_HEALTH_LOG_INTERVAL = 300_000L;
    private long lastQueueHealthLog;

    /** Reconstructible local file/metadata events are coalesced in memory. */
    private final Map<String, PendingUpdate> pendingUpdates = new ConcurrentHashMap<>();

    private static final class PendingUpdate {
        private final ArbitraryTransactionData transactionData;
        private final int flags;

        private PendingUpdate(ArbitraryTransactionData transactionData, int flags) {
            this.transactionData = transactionData;
            this.flags = flags;
        }
    }

    private static final NumberFormat FORMATTER = NumberFormat.getNumberInstance();

    static {
        FORMATTER.setGroupingUsed(true);
    }

    public static synchronized ArbitraryDataCacheManager getInstance() {
        if (instance == null) {
            instance = new ArbitraryDataCacheManager();
        }

        return instance;
    }

    /**
     * Are The Latest Signatures Populated?
     *
     * Assessing if the latest signatures have been cached for QDN data.
     *
     * @param connection a database connection
     *
     * @return true if the signatures have been cached, otherwise false
     *
     * @throws SQLException
     */
    private static boolean isLatestSignaturePopulated(Connection connection) throws SQLException {
        String sql = "SELECT latest_signature_populated FROM DatabaseInfo WHERE latest_signature_populated = 1";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        try (ResultSet rs = preparedStatement.executeQuery()) {
            if (rs.next()) {
                return true;
            }
            else {
                return false;
            }
        }
    }

    /**
     * Populate The Latest Signatures, If Necessary
     *
     * If it hasn't been done already, cache the latest signature for QDN data item.
     *
     * @param connection a connection to the database
     *
     * @throws DataException
     */
    public static void populateLatestSignaturesIfNecessary(Connection connection) throws DataException {

        try {
            int databaseVersion = HSQLDBDatabaseUpdates.fetchDatabaseVersion(connection);

            // if latest signature column is added, but not populated, then populate now
            if( databaseVersion > 51 && !isLatestSignaturePopulated(connection)) {

                SplashFrame.getInstance().updateStatus("Gathering latest signatures for QDN ...");
                LOGGER.info("Gathering latest signatures for QDN ...");

                try (Statement arbitraryTransactionSelectionStatement = connection.createStatement()) {
                    ResultSet resultSet = arbitraryTransactionSelectionStatement.executeQuery(
                            "SELECT signature, service, name, identifier " +
                                    "FROM Transactions t " +
                                    "JOIN ArbitraryTransactions a ON t.signature = a.signature " +
                                    "WHERE name IS NOT NULL AND a.update_method = 0 " +
                                    "ORDER BY created_when DESC "
                    );

                    Map<ArbitraryTransactionDataHashWrapper, byte[]> signatureByData = new HashMap<>();

                    // process arbitrary transaction results
                    while (resultSet.next()) {

                        signatureByData.putIfAbsent(
                                new ArbitraryTransactionDataHashWrapper(
                                        resultSet.getInt(2),
                                        resultSet.getString(3),
                                        resultSet.getString(4)
                                ),
                                resultSet.getBytes(1));
                    }

                    LOGGER.info("Updating {} arbitrary resources with latest signatures", signatureByData.size());

                    String populateSql = "UPDATE ArbitraryResourcesCache SET latest_signature = ? WHERE name = ? AND service = ? AND identifier = ?";
                    PreparedStatement preparedStatement = connection.prepareStatement(populateSql);

                    // preserve the auto commit enabling, so we can return to
                    boolean autoCommit = connection.getAutoCommit();
                    connection.setAutoCommit(false);

                    // for each signature by data pairing, prepare a database update statement and add it to a batch
                    for (Map.Entry<ArbitraryTransactionDataHashWrapper, byte[]> entry : signatureByData.entrySet()) {
                        preparedStatement.setBytes(1, entry.getValue());

                        ArbitraryTransactionDataHashWrapper wrapper = entry.getKey();
                        preparedStatement.setString(2, wrapper.getName());
                        preparedStatement.setInt(3, entry.getKey().getService());

                        String identifier = entry.getKey().getIdentifier();
                        preparedStatement.setString(4, identifier != null ? identifier : "default");

                        preparedStatement.addBatch();
                    }

                    // HSQLDB rejects executeBatch() when no statements were added. This is
                    // expected for a new repository that only contains the genesis block.
                    if (!signatureByData.isEmpty())
                        preparedStatement.executeBatch();

                    LOGGER.info("Updated arbitrary resources with latest signatures");

                    try (Statement stmt = connection.createStatement()) {
                        stmt.execute("UPDATE ArbitraryResourcesCache SET lower_case_name = LCASE(name)");
                    }

                    LOGGER.info("Updated arbitrary resources with lower case names for indexing purposes");

                    // update latest signature populated flag to database info
                    String updateFlagSql = "UPDATE DatabaseInfo SET latest_signature_populated = 1";
                    try (PreparedStatement updateFlagStatement = connection.prepareStatement(updateFlagSql)) {

                        updateFlagStatement.executeUpdate();

                        // verify the change
                        String verifyFlagSql = "SELECT latest_signature_populated FROM DatabaseInfo";
                        try (PreparedStatement verifyFlagStatement = connection.prepareStatement(verifyFlagSql);
                             ResultSet rs = verifyFlagStatement.executeQuery()) {

                            if (rs.next() && rs.getInt("latest_signature_populated") == 1) {
                                LOGGER.info("latest signature populated flag has been set");
                            }
                            else {
                                LOGGER.info("latest signature populated flag has not been set");
                            }
                        }
                    }

                    connection.commit();
                    connection.setAutoCommit(autoCommit);

                    LOGGER.info("arbitrary resources data latest signatures committed");
                }
            }
        } catch (SQLException e) {
            throw new DataException(e.getMessage());
        } finally {
            SplashFrame.getInstance().updateStatus("Proceeding to Start Qortal ...");
        }
    }

    @Override
    public void run() {
        Thread.currentThread().setName("Arbitrary Data Cache Manager");
        Thread.currentThread().setPriority(NORM_PRIORITY);

        try {
            while (!this.isStopping && !Controller.isStopping()) {
                try {
                    Thread.sleep(500L);

                    persistPendingUpdates();

                    // Process queue
                    processResourceQueue();
                } catch (InterruptedException e) {
                    // Check if we're shutting down
                    if (this.isStopping || Controller.isStopping()) {
                        LOGGER.info("Arbitrary Data Cache Manager shutting down");
                        break;
                    }
                    LOGGER.warn("Arbitrary Data Cache Manager interrupted, retrying...", e);
                } catch (Exception e) {
                    LOGGER.error(e.getMessage(), e);
                    Thread.sleep(5_000L);
                }
            }

            // Give transient repository conflicts a few chances to clear before exit.
            for (int attempt = 0; attempt < 3 && !this.pendingUpdates.isEmpty(); ++attempt) {
                if (persistPendingUpdates())
                    break;
                Thread.sleep(100L);
            }
            processResourceQueue();
        } catch (InterruptedException e) {
            if (!this.isStopping && !Controller.isStopping()) {
                LOGGER.error("Arbitrary Data Cache Manager interrupted unexpectedly", e);
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    private boolean persistPendingUpdates() {
        if (this.pendingUpdates.isEmpty())
            return true;

        // Dirty markers written by block processing are part of the authoritative block
        // transaction. Serialize this derived-cache writer with chain changes so HSQLDB
        // never has to choose one of those transactions as a serialization victim.
        ReentrantLock blockchainLock = Controller.getInstance().getBlockchainLock();
        if (shouldYieldToChainWork(blockchainLock) || !blockchainLock.tryLock())
            return false;

        List<Map.Entry<String, PendingUpdate>> snapshot = new ArrayList<>(UPDATE_BATCH_SIZE);
        try {
            for (Map.Entry<String, PendingUpdate> entry : this.pendingUpdates.entrySet()) {
                snapshot.add(entry);
                if (snapshot.size() >= UPDATE_BATCH_SIZE)
                    break;
            }

            try (Repository repository = RepositoryManager.getRepository()) {
                for (Map.Entry<String, PendingUpdate> entry : snapshot) {
                    PendingUpdate update = entry.getValue();
                    repository.getArbitraryRepository().markResourceDirty(update.transactionData, update.flags);
                }
                repository.saveChanges();

                // remove(key, value) preserves any event that arrived while this snapshot committed
                snapshot.forEach(entry -> this.pendingUpdates.remove(entry.getKey(), entry.getValue()));
                return this.pendingUpdates.isEmpty();
            } catch (DataException e) {
                LOGGER.warn("Unable to persist {} pending QDN cache updates; retaining them for retry",
                        snapshot.size(), e);
                return false;
            }
        } finally {
            blockchainLock.unlock();
        }
    }

    public void shutdown() {
        isStopping = true;
        this.interrupt();
    }


    private void processResourceQueue() {
        ReentrantLock blockchainLock = Controller.getInstance().getBlockchainLock();
        if (shouldYieldToChainWork(blockchainLock))
            return;

        List<ArbitraryResourceDirtyData> dirtyResources;
        try (final Repository repository = RepositoryManager.getRepository()) {
            long now = System.currentTimeMillis();
            logQueueHealthIfDue(repository, now);
            dirtyResources = repository.getArbitraryRepository()
                    .getDirtyResources(UPDATE_BATCH_SIZE, now);
            repository.discardChanges();
        } catch (DataException e) {
            LOGGER.error("Repository issue while reading arbitrary resource cache queue", e);
            return;
        }

        if (dirtyResources.isEmpty())
            return;

        int completed = 0;
        for (ArbitraryResourceDirtyData dirtyData : dirtyResources) {
            // Commit each resource independently. Previously the first row remained write
            // locked while the worker scanned metadata for the rest of a 100-row batch.
            if (shouldYieldToChainWork(blockchainLock) || !blockchainLock.tryLock())
                break;

            try {
                if (Synchronizer.getInstance().isSynchronizing()
                        || Synchronizer.getInstance().isSyncRequested())
                    break;

                if (processDirtyResourceTransaction(dirtyData))
                    completed++;
                else
                    break;
            } finally {
                blockchainLock.unlock();
            }
        }

        LOGGER.debug("Processed {} of {} durable QDN cache updates using independent commits",
                completed, dirtyResources.size());
    }

    /**
     * Process and acknowledge exactly one revision atomically. Newer revisions cannot be
     * cleared because clearDirtyResource() includes the observed revision in its predicate.
     */
    private boolean processDirtyResourceTransaction(ArbitraryResourceDirtyData dirtyData) {
        try (Repository repository = RepositoryManager.getRepository()) {
            try {
                ArbitraryTransactionData transactionData = processDirtyResource(repository, dirtyData);
                repository.getArbitraryRepository().clearDirtyResource(dirtyData);

                if (transactionData != null) {
                    repository.afterCommit(() -> EventBus.INSTANCE.notify(
                        new DataMonitorEvent(
                            System.currentTimeMillis(),
                            transactionData.getIdentifier(),
                            transactionData.getName(),
                            transactionData.getService().name(),
                            "updated resource cache and status, queue",
                            transactionData.getTimestamp(),
                            transactionData.getTimestamp()
                        )
                    ));
                }
                repository.saveChanges();
                return true;
            } catch (Exception e) {
                repository.discardChanges();
                if (RepositoryManager.isDeadlockRelated(e)) {
                    LOGGER.debug("QDN cache update conflicted with another database writer; retained it for retry");
                    return false;
                }

                deferDirtyResource(dirtyData, e);
                return true;
            }
        } catch (DataException e) {
            LOGGER.error("Repository issue while processing arbitrary resource cache update", e);
            return false;
        }
    }

    private void deferDirtyResource(ArbitraryResourceDirtyData dirtyData, Exception originalException) {
        long retryDelay = Math.min(MAX_RETRY_DELAY,
                1_000L << Math.min(dirtyData.getRetryCount(), 12));
        try (Repository retryRepository = RepositoryManager.getRepository()) {
            retryRepository.getArbitraryRepository().deferDirtyResource(dirtyData,
                    System.currentTimeMillis() + retryDelay);
            retryRepository.saveChanges();
        } catch (DataException deferException) {
            originalException.addSuppressed(deferException);
            LOGGER.warn("Unable to defer failed QDN cache update; retained it for immediate retry", originalException);
            return;
        }

        int failedAttempts = dirtyData.getRetryCount() + 1;
        if (failedAttempts >= 10 && (failedAttempts == 10 || failedAttempts % 12 == 0)) {
            LOGGER.error("QDN cache update for {} {} {} has failed {} times; work remains queued (retry in {} ms)",
                    dirtyData.getService(), dirtyData.getName(), dirtyData.getIdentifier(),
                    failedAttempts, retryDelay, originalException);
        } else {
            LOGGER.warn("Unable to update QDN cache for {} {} {} (retry in {} ms)",
                    dirtyData.getService(), dirtyData.getName(), dirtyData.getIdentifier(), retryDelay, originalException);
        }
    }

    private boolean shouldYieldToChainWork(ReentrantLock blockchainLock) {
        Synchronizer synchronizer = Synchronizer.getInstance();
        return synchronizer.isSynchronizing() || synchronizer.isSyncRequested()
                || blockchainLock.hasQueuedThreads();
    }

    private void logQueueHealthIfDue(Repository repository, long now) throws DataException {
        if (now - this.lastQueueHealthLog < QUEUE_HEALTH_LOG_INTERVAL)
            return;

        this.lastQueueHealthLog = now;
        String sql = "SELECT COUNT(*), " +
                "COALESCE(SUM(CASE WHEN retry_count > 0 THEN 1 ELSE 0 END), 0), " +
                "COALESCE(MAX(retry_count), 0), COALESCE(MIN(updated_when), 0) " +
                "FROM ArbitraryResourceDirty";
        try (Statement statement = repository.getConnection().createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            if (!resultSet.next())
                return;

            long pending = resultSet.getLong(1);
            long retrying = resultSet.getLong(2);
            int maxRetries = resultSet.getInt(3);
            long oldestUpdated = resultSet.getLong(4);
            long oldestAge = oldestUpdated > 0 ? Math.max(0L, now - oldestUpdated) : 0L;
            if (retrying > 0) {
                LOGGER.warn("QDN cache queue health: {} pending, {} retrying, max retries {}, oldest age {} ms",
                        pending, retrying, maxRetries, oldestAge);
            } else if (pending >= 1_000) {
                LOGGER.info("QDN cache queue backlog: {} pending, oldest age {} ms", pending, oldestAge);
            }
        } catch (SQLException e) {
            throw new DataException("Unable to inspect QDN cache queue health", e);
        }
    }

    private ArbitraryTransactionData processDirtyResource(Repository repository,
                                                           ArbitraryResourceDirtyData dirtyData) throws DataException {
        String identifier = "default".equals(dirtyData.getIdentifier()) ? null : dirtyData.getIdentifier();
        ArbitraryTransactionData transactionData;

        if (dirtyData.hasFlag(ArbitraryResourceDirtyData.HEAD_DIRTY)) {
            transactionData = repository.getArbitraryRepository().getLatestTransaction(
                    dirtyData.getName(), dirtyData.getService(), null, identifier);
            if (transactionData == null) {
                repository.getArbitraryRepository().delete(
                        new ArbitraryResourceData(dirtyData.getService(), dirtyData.getName(), identifier));
                return null;
            }
        } else {
            byte[] signature = dirtyData.getTriggeringSignature();
            byte[] latestSignature = repository.getArbitraryRepository().getLatestSignature(
                    dirtyData.getService(), dirtyData.getName(), identifier);
            if (signature == null || !Arrays.equals(signature, latestSignature))
                return null; // A stale file/metadata event cannot change the current resource.

            transactionData = repository.getArbitraryRepository().getSingleTransactionBySignature(signature);
            if (transactionData == null)
                return null;
        }

        ArbitraryTransaction arbitraryTransaction = new ArbitraryTransaction(repository, transactionData);
        if (dirtyData.hasFlag(ArbitraryResourceDirtyData.HEAD_DIRTY)
                || dirtyData.hasFlag(ArbitraryResourceDirtyData.METADATA_DIRTY)) {
            arbitraryTransaction.updateArbitraryResourceCacheIncludingMetadata(repository,
                    Set.of(new ArbitraryTransactionDataHashWrapper(transactionData)), new HashMap<>(0));
        }
        if (dirtyData.hasFlag(ArbitraryResourceDirtyData.STATUS_DIRTY)) {
            arbitraryTransaction.updateArbitraryResourceStatus(repository);
        }
        return transactionData;
    }

    public void addToUpdateQueue(ArbitraryTransactionData transactionData) {
        int flags = ArbitraryResourceDirtyData.METADATA_DIRTY | ArbitraryResourceDirtyData.STATUS_DIRTY;
        mergePendingUpdate(transactionData, flags);
    }

    private void mergePendingUpdate(ArbitraryTransactionData transactionData, int flags) {
        if (transactionData == null || transactionData.getName() == null
                || transactionData.getService() == null || flags == 0)
            return;

        ArbitraryResourceStateRegistry.getInstance().invalidate(transactionData.getService(),
                transactionData.getName(), transactionData.getIdentifier());

        String identifier = transactionData.getIdentifier() != null ? transactionData.getIdentifier() : "default";
        String key = transactionData.getService().value + "\u0000"
                + transactionData.getName().toLowerCase(Locale.ROOT) + "\u0000" + identifier;
        this.pendingUpdates.compute(key, (ignored, existing) -> new PendingUpdate(transactionData,
                flags | (existing != null ? existing.flags : 0)));
    }

    /** Persist non-reconstructible events immediately. */
    public void addToUpdateQueue(ArbitraryTransactionData transactionData, int flags) {
        if (transactionData != null)
            ArbitraryResourceStateRegistry.getInstance().invalidate(transactionData.getService(),
                    transactionData.getName(), transactionData.getIdentifier());
        ReentrantLock blockchainLock = Controller.getInstance().getBlockchainLock();
        if (shouldYieldToChainWork(blockchainLock) || !blockchainLock.tryLock()) {
            mergePendingUpdate(transactionData, flags);
            return;
        }

        try {
            try (Repository repository = RepositoryManager.getRepository()) {
                repository.getArbitraryRepository().markResourceDirty(transactionData, flags);
                repository.saveChanges();
            } catch (DataException e) {
                mergePendingUpdate(transactionData, flags);
                LOGGER.warn("Unable to persist QDN cache update for transaction {}; retained it for retry",
                        transactionData != null && transactionData.getSignature() != null
                                ? Base58.encode(transactionData.getSignature()) : "unknown", e);
            }
        } finally {
            blockchainLock.unlock();
        }
    }

    public boolean needsArbitraryResourcesCacheRebuild(Repository repository) throws DataException {
        return repository.getArbitraryRepository().isOldestNamedResourceMissingFromCache();
    }

    public boolean buildArbitraryResourcesCache(Repository repository, boolean forceRebuild) throws DataException {
        if (Settings.getInstance().isLite()) {
            // Lite nodes have no blockchain
            return false;
        }

        try {
            // Skip if already built
            if (!needsArbitraryResourcesCacheRebuild(repository) && !forceRebuild) {
                LOGGER.debug("Arbitrary resources cache already built");
                return false;
            }

            LOGGER.info("Building arbitrary resources cache...");
            SplashFrame.getInstance().updateStatus("Building QDN cache - please wait...");

            final int batchSize = Settings.getInstance().getBuildArbitraryResourcesBatchSize();
            int offset = 0;

            List<ArbitraryTransactionData> allArbitraryTransactionsInDescendingOrder
                    = repository.getArbitraryRepository().getLatestArbitraryTransactions();

            LOGGER.info("arbitrary transactions: count = " + allArbitraryTransactionsInDescendingOrder.size());

            List<ArbitraryResourceData> resources = repository.getArbitraryRepository().getArbitraryResources(null, null, true);

            Map<ArbitraryTransactionDataHashWrapper, ArbitraryResourceData> resourceByWrapper = new HashMap<>(resources.size());
            for( ArbitraryResourceData resource : resources ) {
                resourceByWrapper.put(
                    new ArbitraryTransactionDataHashWrapper(resource.service.value, resource.name, resource.identifier),
                    resource
                );
            }

            LOGGER.info("arbitrary resources: count = " + resourceByWrapper.size());

            Set<ArbitraryTransactionDataHashWrapper> latestTransactionsWrapped = new HashSet<>(allArbitraryTransactionsInDescendingOrder.size());

            // Loop through all ARBITRARY transactions, and determine latest state
            while (!Controller.isStopping()) {
                LOGGER.info(
                    "Fetching arbitrary transactions {} - {} / {} Total",
                    FORMATTER.format(offset),
                    FORMATTER.format(offset+batchSize-1),
                    FORMATTER.format(allArbitraryTransactionsInDescendingOrder.size())
                );

                List<ArbitraryTransactionData> transactionsToProcess
                    = allArbitraryTransactionsInDescendingOrder.stream()
                        .skip(offset)
                        .limit(batchSize)
                        .collect(Collectors.toList());

                if (transactionsToProcess.isEmpty()) {
                    // Complete
                    break;
                }

                try {
                    for( ArbitraryTransactionData transactionData : transactionsToProcess) {
                        if (transactionData.getService() == null) {
                            // Unsupported service - ignore this resource
                            continue;
                        }

                        latestTransactionsWrapped.add(new ArbitraryTransactionDataHashWrapper(transactionData));

                        // Update arbitrary resource caches
                        ArbitraryTransaction arbitraryTransaction = new ArbitraryTransaction(repository, transactionData);
                        arbitraryTransaction.updateArbitraryResourceCacheIncludingMetadata(repository, latestTransactionsWrapped, resourceByWrapper);
                    }
                    repository.saveChanges();
                } catch (DataException e) {
                    repository.discardChanges();

                    LOGGER.error(e.getMessage(), e);
                }
                offset += batchSize;
            }

            refreshArbitraryStatuses(repository);
            LOGGER.info("Completed build of arbitrary resources cache.");
            return true;
        }
        catch (DataException e) {
            LOGGER.info("Unable to build arbitrary resources cache: {}. The database may have been left in an inconsistent state.", e.getMessage());

            // Throw an exception so that the node startup is halted, allowing for a retry next time.
            repository.discardChanges();
            throw new DataException("Build of arbitrary resources cache failed.");
        }
        catch (Exception e) {
            LOGGER.error(e.getMessage(), e);

            return false;
        }
    }

    private boolean refreshArbitraryStatuses(Repository repository) throws DataException {
        try {
            LOGGER.info("Refreshing arbitrary resource statuses for locally hosted transactions...");
            SplashFrame.getInstance().updateStatus("Refreshing statuses - please wait...");

            final int batchSize = Settings.getInstance().getBuildArbitraryResourcesBatchSize();
            int offset = 0;
            List<ArbitraryTransactionData> allHostedTransactions =
                    ArbitraryDataStorageManager.getInstance().listAllHostedTransactions(repository, null, null);

            while (!Controller.isStopping()) {
                LOGGER.info("Fetching hosted transactions {} - {} / {} Total",
                        FORMATTER.format(offset), FORMATTER.format(offset + batchSize - 1),
                        FORMATTER.format(allHostedTransactions.size()));

                List<ArbitraryTransactionData> hostedTransactions = allHostedTransactions.stream()
                        .skip(offset)
                        .limit(batchSize)
                        .collect(Collectors.toList());
                if (hostedTransactions.isEmpty())
                    break;

                try {
                    for (ArbitraryTransactionData transactionData : hostedTransactions) {
                        ArbitraryTransaction arbitraryTransaction = new ArbitraryTransaction(repository, transactionData);
                        arbitraryTransaction.updateArbitraryResourceStatus(repository);
                    }
                    repository.saveChanges();
                } catch (DataException e) {
                    repository.discardChanges();
                    LOGGER.error(e.getMessage(), e);
                }
                offset += batchSize;
            }

            LOGGER.info("Completed refresh of arbitrary resource statuses.");
            return true;
        } catch (DataException e) {
            LOGGER.info("Unable to refresh arbitrary resource statuses: {}. The database may have been left in an inconsistent state.",
                    e.getMessage());
            repository.discardChanges();
            throw new DataException("Refresh of arbitrary resource statuses failed.");
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return false;
        }
    }

}
