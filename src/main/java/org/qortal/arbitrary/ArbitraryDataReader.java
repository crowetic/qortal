package org.qortal.arbitrary;

import java.io.File;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.qortal.arbitrary.ArbitraryDataFile.ResourceIdType;
import org.qortal.arbitrary.exception.MissingDataException;
import org.qortal.arbitrary.misc.Service;
import org.qortal.controller.arbitrary.ArbitraryDataBuildManager;
import org.qortal.controller.arbitrary.ArbitraryResourceStateRegistry;
import org.qortal.controller.arbitrary.ArbitraryDataManager;
import org.qortal.crypto.AES;
import org.qortal.data.transaction.ArbitraryTransactionData;
import org.qortal.data.transaction.ArbitraryTransactionData.Compression;
import org.qortal.data.transaction.ArbitraryTransactionData.DataType;
import org.qortal.repository.DataException;
import org.qortal.repository.Repository;
import org.qortal.repository.RepositoryManager;
import org.qortal.settings.Settings;
import org.qortal.transform.Transformer;
import org.qortal.utils.ArbitraryTransactionUtils;
import org.qortal.utils.Base58;
import org.qortal.utils.FilesystemUtils;
import org.qortal.utils.ListUtils;
import org.qortal.utils.StringUtils;
import org.qortal.utils.ZipUtils;

public class ArbitraryDataReader {

    private static final Logger LOGGER = LogManager.getLogger(ArbitraryDataReader.class);

    private final String resourceId;
    private final ResourceIdType resourceIdType;
    private final Service service;
    private final String identifier;
    private ArbitraryTransactionData transactionData;
    private String secret58;
    private Path filePath;
    private boolean canRequestMissingFiles;

    // Intermediate paths
    private final Path workingPath;
    private final Path uncompressedPath;

    // Stats (available for synchronous builds only)
    private int layerCount;
    private byte[] latestSignature;

    // The resource being read
    ArbitraryDataResource arbitraryDataResource = null;

    // Track if decrypt+unzip was combined in a single pass (optimization)
    private boolean decryptAndUnzipCombined = false;

    // Ownership covers cache checks, preparation, extraction and validation. Key by the actual
    // working path so different callers that address the same output cannot build it concurrently.
    private static final ConcurrentHashMap<Path, CompletableFuture<LoadResult>> inProgress = new ConcurrentHashMap<>();

    private static final class LoadResult {
        private final Path path;
        private final int layers;
        private final byte[] signature;
        private final boolean rebuilt;

        private LoadResult(ArbitraryDataReader reader, boolean rebuilt) {
            this.path = reader.filePath;
            this.layers = reader.layerCount;
            this.signature = reader.latestSignature != null ? reader.latestSignature.clone() : null;
            this.rebuilt = rebuilt;
        }

        private void applyTo(ArbitraryDataReader reader) {
            reader.filePath = this.path;
            reader.layerCount = this.layers;
            reader.latestSignature = this.signature != null ? this.signature.clone() : null;
        }
    }

    private Path loadKey() {
        return this.workingPath.toAbsolutePath().normalize();
    }

    public ArbitraryDataReader(String resourceId, ResourceIdType resourceIdType, Service service, String identifier) throws DataException {
        // Ensure names are always lowercase
        if (resourceIdType == ResourceIdType.NAME) {
            resourceId = resourceId.toLowerCase();
        }

        // If identifier is a blank string, or reserved keyword "default", treat it as null
        if (identifier == null || identifier.isEmpty() || identifier.equals("default")) {
            identifier = null;
        }

        this.resourceId = resourceId;
        this.resourceIdType = resourceIdType;
        this.service = service;
        this.identifier = identifier;

        this.workingPath = this.buildWorkingPath();
        this.uncompressedPath = Paths.get(this.workingPath.toString(), "data");

        // By default we can request missing files
        // Callers can use setCanRequestMissingFiles(false) to prevent it
        this.canRequestMissingFiles = true;
    }

    private Path buildWorkingPath() throws DataException {
        // Use the user-specified temp dir, as it is deterministic, and is more likely to be located on reusable storage hardware
        String baseDir = Settings.getInstance().getTempDataPath();
        String identifier = this.identifier != null ?  this.identifier : "default";

        try {
            return Paths.get(baseDir, "reader", this.resourceIdType.toString(), StringUtils.sanitizeString(this.resourceId), this.service.toString(), StringUtils.sanitizeString(identifier));
        } catch (InvalidPathException e) {
            throw new DataException(String.format("Invalid path: %s", e.getMessage()));
        }
    }

    public boolean isCachedDataAvailable() {
        // If this resource is in the build queue then we shouldn't attempt to serve
        // cached data, as it may not be fully built
        if (this.isBuilding()) {
            return false;
        }

        // Not in the build queue - so check the cache itself
        ArbitraryDataCache cache = new ArbitraryDataCache(this.uncompressedPath, false,
                this.resourceId, this.resourceIdType, this.service, this.identifier);
        if (cache.isCachedDataAvailable()) {
            this.filePath = this.uncompressedPath;
            return true;
        }
        return false;
    }

    public boolean isBuilding() {
        return inProgress.containsKey(this.loadKey()) || ArbitraryDataBuildManager.getInstance()
                .isInBuildQueue(this.resourceId, this.resourceIdType, this.service, this.identifier);
    }

    private ArbitraryDataBuildQueueItem createQueueItem() {
        return new ArbitraryDataBuildQueueItem(this.resourceId, this.resourceIdType, this.service, this.identifier);
    }

    private ArbitraryDataResource createArbitraryDataResource() {
        return new ArbitraryDataResource(this.resourceId, this.resourceIdType, this.service, this.identifier);
    }


    /**
     * loadAsynchronously
     *
     * Attempts to load the resource asynchronously
     * This adds the build task to a queue, and the result will be cached when complete
     * To check the status of the build, periodically call isCachedDataAvailable()
     * Once it returns true, you can then use getFilePath() to access the data itself.
     *
     * @param overwrite - set to true to force rebuild an existing cache
     * @return true if added or already present in queue; false if not
     */
    public boolean loadAsynchronously(boolean overwrite, int priority) {
        if (inProgress.containsKey(this.loadKey()))
            return true;
        ArbitraryDataCache cache = new ArbitraryDataCache(this.uncompressedPath, overwrite,
                this.resourceId, this.resourceIdType, this.service, this.identifier);
        if (cache.isCachedDataAvailable()) {
            // Use cached data
            this.filePath = this.uncompressedPath;
            return true;
        }

        ArbitraryDataBuildQueueItem item = this.createQueueItem();
        item.setPriority(priority);
        return ArbitraryDataBuildManager.getInstance().addToBuildQueue(item);
    }

    /**
     * loadSynchronously
     *
     * Attempts to load the resource synchronously
     * Warning: this can block for a long time when building or fetching complex data
     * If no exception is thrown, you can then use getFilePath() to access the data immediately after returning
     *
     * @param overwrite - set to true to force rebuild an existing cache
     */
    public void loadSynchronously(boolean overwrite) throws DataException, IOException, MissingDataException {
        this.loadSynchronously(overwrite, this.loadWaitTimeoutMillis());
    }

    /**
     * API callers must respect builds already queued by the scheduler. Once a build
     * starts, join its future; do not spin or start a second build outside the queue.
     * Preserve the synchronous API's wait-for-completion behavior without imposing
     * a new deadline. Both queue and active-build waits remain interruptible.
     */
    public void loadSynchronouslyWhenReady(boolean overwrite, java.util.function.BooleanSupplier stopping)
            throws DataException, IOException, MissingDataException {
        this.loadSynchronously(overwrite, 0, stopping);
    }

    /** Bound a request's queue/join wait without cancelling the owning build. */
    public void loadSynchronouslyWhenReady(boolean overwrite, java.util.function.BooleanSupplier stopping,
            long timeoutMillis) throws DataException, IOException, MissingDataException {
        if (timeoutMillis <= 0)
            throw new IllegalArgumentException("Wait timeout must be positive");
        long started = System.nanoTime();
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        java.util.concurrent.atomic.AtomicBoolean timedOut = new java.util.concurrent.atomic.AtomicBoolean();
        java.util.function.BooleanSupplier expired = () -> {
            boolean elapsed = System.nanoTime() - started >= timeoutNanos;
            if (elapsed) timedOut.set(true);
            return elapsed;
        };
        this.loadSynchronously(overwrite, timeoutMillis, () -> stopping.getAsBoolean() || expired.getAsBoolean());
        if (stopping.getAsBoolean())
            throw new DataException("Node is stopping");
        if (timedOut.get())
            throw new DataException("Timed out waiting for QDN data", new TimeoutException("QDN request wait expired"));
    }

    private void waitForQueuedBuild(java.util.function.BooleanSupplier stopping) throws DataException {
        while (!stopping.getAsBoolean() && !inProgress.containsKey(this.loadKey()) && this.isQueuedForBuild()) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DataException("Interrupted while waiting for queued load", e);
            }
        }
    }

    boolean isQueuedForBuild() {
        return ArbitraryDataBuildManager.getInstance().isInBuildQueue(
                this.resourceId, this.resourceIdType, this.service, this.identifier);
    }

    /**
     * Load with a timeout for joining an existing build. Zero waits until that build finishes
     * (still interruptibly), so a queue worker does not report a healthy long build as failed.
     */
    public void loadSynchronously(boolean overwrite, long waitTimeoutMillis) throws DataException, IOException, MissingDataException {
        this.loadSynchronously(overwrite, waitTimeoutMillis, null);
    }

    private void loadSynchronously(boolean overwrite, long waitTimeoutMillis,
            java.util.function.BooleanSupplier stopping) throws DataException, IOException, MissingDataException {
        if (waitTimeoutMillis < 0)
            throw new IllegalArgumentException("Wait timeout must not be negative");
        Path key = this.loadKey();
        CompletableFuture<LoadResult> started = new CompletableFuture<>();
        while (true) {
            if (stopping != null) {
                // Recheck the queue after joining a cache hit that did not satisfy
                // a forced rebuild, as well as before the initial attempt.
                this.waitForQueuedBuild(stopping);
                if (stopping.getAsBoolean())
                    return;
            }
            CompletableFuture<LoadResult> existing = inProgress.putIfAbsent(key, started);
            if (existing == null)
                break;
            LoadResult result = this.awaitLoad(existing, waitTimeoutMillis, stopping);
            if (result == null) // This waiter stopped; the owning build is unaffected.
                return;
            if (!overwrite || result.rebuilt) {
                result.applyTo(this);
                return;
            }
            // A concurrent cache check does not satisfy a forced rebuild. Its work has
            // completed, so release only that completed claim and compete for the next one.
            inProgress.remove(key, existing);
        }

        boolean building = false;
        try {
            if (!this.loadFromCache(overwrite)) {
                building = true;
                this.invalidateResourceStatus();
                this.loadOwned();
            }
            started.complete(new LoadResult(this, building));
        } catch (DataException | IOException | MissingDataException | RuntimeException | Error e) {
            started.completeExceptionally(e);
            throw e;
        } finally {
            inProgress.remove(key, started);
            if (building)
                this.invalidateResourceStatus();
        }
    }

    private LoadResult awaitLoad(CompletableFuture<LoadResult> existing, long timeoutMillis,
            java.util.function.BooleanSupplier stopping) throws DataException, IOException, MissingDataException {
        try {
            if (stopping == null)
                return timeoutMillis == 0 ? existing.get() : existing.get(timeoutMillis, TimeUnit.MILLISECONDS);
            long started = System.nanoTime();
            while (!stopping.getAsBoolean()) {
                long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
                if (timeoutMillis > 0 && elapsed >= timeoutMillis)
                    throw new TimeoutException();
                long wait = timeoutMillis == 0 ? 50 : Math.min(50, timeoutMillis - elapsed);
                try {
                    return existing.get(wait, TimeUnit.MILLISECONDS);
                } catch (TimeoutException stillBuilding) {
                    // Observe shutdown without cancelling or releasing another caller's build.
                }
            }
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DataException("Interrupted while waiting for concurrent load", e);
        } catch (TimeoutException e) {
            // This caller owns no files and must not cancel or clean up the active build.
            throw new DataException("Timeout waiting for concurrent load to complete", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof DataException)
                throw (DataException) cause;
            if (cause instanceof IOException)
                throw (IOException) cause;
            if (cause instanceof MissingDataException)
                throw (MissingDataException) cause;
            if (cause instanceof RuntimeException)
                throw (RuntimeException) cause;
            if (cause instanceof Error)
                throw (Error) cause;
            throw new DataException("Concurrent load failed", cause);
        }
    }

    long loadWaitTimeoutMillis() {
        return 60_000L;
    }

    boolean loadFromCache(boolean overwrite) {
        ArbitraryDataCache cache = new ArbitraryDataCache(this.uncompressedPath, overwrite,
                this.resourceId, this.resourceIdType, this.service, this.identifier);
        if (!cache.isCachedDataAvailable())
            return false;
        this.filePath = this.uncompressedPath;
        return true;
    }

    private void invalidateResourceStatus() {
        if (this.resourceIdType == ResourceIdType.NAME)
            ArbitraryResourceStateRegistry.getInstance().invalidate(this.service, this.resourceId, this.identifier);
    }

    private void loadOwned() throws DataException, IOException, MissingDataException {
        boolean buildStarted = false;
        try {
            this.arbitraryDataResource = this.createArbitraryDataResource();
            this.checkEnabled();
            ArbitraryDataBuildManager.getInstance().setBuildInProgress(true);
            buildStarted = true;
            this.preExecute();
            this.decryptAndUnzipCombined = false;
            this.fetch();
            this.decrypt();
            if (!this.decryptAndUnzipCombined)
                this.uncompress();
            this.validate();
        } catch (DataException | IOException | MissingDataException | RuntimeException | Error e) {
            if (buildStarted)
                this.deleteWorkingDirectory();
            throw e;
        } finally {
            if (buildStarted)
                this.postExecute();
        }
    }

    private void preExecute() throws DataException {
        this.createWorkingDirectory();
        this.createUncompressedDirectory();
    }

    private void postExecute() {
        ArbitraryDataBuildManager.getInstance().setBuildInProgress(false);
    }

    private void checkEnabled() throws DataException {
        if (!Settings.getInstance().isQdnEnabled()) {
            throw new DataException("QDN is disabled in settings");
        }
    }

    private void createWorkingDirectory() throws DataException {
        try {
            Files.createDirectories(this.workingPath);
        } catch (IOException e) {
            throw new DataException(String.format("Unable to create temp directory %s: %s", this.workingPath, e.getMessage()));
        }
    }

    /**
     * Working directory should only be deleted on failure, since it is currently used to
     * serve a cached version of the resource for subsequent requests.
     */
    private void deleteWorkingDirectory() {
        try {
            if (Files.isDirectory(this.workingPath))
                FilesystemUtils.safeDeleteDirectory(this.workingPath, true);
        } catch (IOException | IllegalArgumentException e) {
            // Ignore failures as this isn't an essential step
            LOGGER.info("Unable to delete working path {}: {}", this.workingPath, e.getMessage());
        }
    }

    private void createUncompressedDirectory() throws DataException {
        try {
            // Create parent directory
            Files.createDirectories(this.uncompressedPath.getParent());
            // Ensure child directory doesn't already exist
            FileUtils.deleteDirectory(this.uncompressedPath.toFile());

        } catch (IOException e) {
            throw new DataException("Unable to create uncompressed directory");
        }
    }

    void fetch() throws DataException, IOException, MissingDataException {
        switch (resourceIdType) {

            case FILE_HASH:
                this.fetchFromFileHash();
                break;

            case NAME:
                this.fetchFromName();
                break;

            case SIGNATURE:
                this.fetchFromSignature();
                break;

            case TRANSACTION_DATA:
                this.fetchFromTransactionData(this.transactionData);
                break;

            default:
                throw new DataException(String.format("Unknown resource ID type specified: %s", resourceIdType));
        }
    }

    private void fetchFromFileHash() throws DataException {
        // Load data file directly from the hash (without a signature)
        ArbitraryDataFile arbitraryDataFile = ArbitraryDataFile.fromHash58(resourceId, null);
        // Set filePath to the location of the ArbitraryDataFile
        this.filePath = arbitraryDataFile.getFilePath();
    }

    private void fetchFromName() throws DataException, IOException, MissingDataException {
        try {
            // Build the existing state using past transactions
            ArbitraryDataBuilder builder = new ArbitraryDataBuilder(this.resourceId, this.service, this.identifier);
            builder.build();
            
            Path builtPath = builder.getFinalPath();
            if (builtPath == null) {
                throw new DataException("Unable to build path");
            }

            // Update stats
            this.layerCount = builder.getLayerCount();
            this.latestSignature = builder.getLatestSignature();

            // Set filePath to the builtPath
            this.filePath = builtPath;

        } catch (InvalidObjectException e) {
            // Hash validation failed. Invalidate the cache for this name, so it can be rebuilt
            LOGGER.info("Deleting {}", this.workingPath.toString());
            FilesystemUtils.safeDeleteDirectory(this.workingPath, false);
            throw(e);
        }
    }

    private void fetchFromSignature() throws DataException, IOException, MissingDataException {
        // Load the full transaction data from the database so we can access the file hashes
        ArbitraryTransactionData transactionData;
        try (final Repository repository = RepositoryManager.getRepository()) {
            transactionData = (ArbitraryTransactionData) repository.getTransactionRepository().fromSignature(Base58.decode(resourceId));
        }
        
        if (transactionData == null) {
            throw new DataException(String.format("Transaction data not found for signature %s", this.resourceId));
        }

        this.fetchFromTransactionData(transactionData);
    }

    private void fetchFromTransactionData(ArbitraryTransactionData transactionData) throws DataException, IOException, MissingDataException {
        if (transactionData == null) {
            throw new DataException(String.format("Transaction data not found for signature %s", this.resourceId));
        }

        // Load secret
        byte[] secret = transactionData.getSecret();
        if (secret != null) {
            this.secret58 = Base58.encode(secret);
        }

        // Load data file(s)
        ArbitraryDataFile arbitraryDataFile = ArbitraryDataFile.fromTransactionData(transactionData);
        ArbitraryTransactionUtils.checkAndRelocateMiscFiles(transactionData);
        
        if (arbitraryDataFile == null) {
            throw new DataException(String.format("arbitraryDataFile is null"));
        }

        if (!arbitraryDataFile.allFilesExist()) {
            if (ListUtils.isNameBlocked(transactionData.getName())) {
                throw new DataException(
                        String.format("Unable to request missing data for file %s because the name is blocked", arbitraryDataFile));
            } else {
                // Ask the arbitrary data manager to fetch data for this transaction
                String message;
                if (this.canRequestMissingFiles) {
                    boolean requested = ArbitraryDataManager.getInstance().fetchData(transactionData);

                    if (requested) {
                        message = String.format("Requested missing data for file %s", arbitraryDataFile);
                    } else {
                        message = String.format("Unable to reissue request for missing file %s for signature %s due to rate limit. Please try again later.", arbitraryDataFile, Base58.encode(transactionData.getSignature()));
                    }
                } else {
                    message = String.format("Missing data for file %s", arbitraryDataFile);
                }

                // Throw a missing data exception, which allows subsequent layers to fetch data
                LOGGER.trace(message);
                throw new MissingDataException(message);
            }
        }

        // Data hashes need some extra processing
        if (transactionData.getDataType() == DataType.DATA_HASH) {
            if (arbitraryDataFile.allChunksExist() && !arbitraryDataFile.exists()) {
                // We have all the chunks but not the complete file, so join them
                arbitraryDataFile.join();
            }

            // If the complete file still doesn't exist then something went wrong
            if (!arbitraryDataFile.exists()) {
                throw new IOException(String.format("File doesn't exist: %s", arbitraryDataFile));
            }
            
            // Ensure the complete hash matches the joined chunks
            if (!Arrays.equals(arbitraryDataFile.digest(), transactionData.getData())) {
                // Delete the invalid file
                LOGGER.info("Deleting invalid file: path = {}", arbitraryDataFile.getFilePath());
                if (arbitraryDataFile.delete()) {
                    LOGGER.info("Deleted invalid file successfully: path = {}", arbitraryDataFile.getFilePath());
                } else {
                    LOGGER.warn("Could not delete invalid file: path = {}", arbitraryDataFile.getFilePath());
                }

                // Also delete its chunks
                if (arbitraryDataFile.deleteAllChunks()) {
                    LOGGER.info("Deleted all chunks associated with invalid file: {}", arbitraryDataFile.getFilePath());
                } else {
                    LOGGER.warn("Failed to delete one or more chunks for invalid file: {}", arbitraryDataFile.getFilePath());
                }

                throw new DataException("Unable to validate complete file hash");
            }
        }

        // Ensure the file's size matches the size reported by the transaction (throws a DataException if not)
        arbitraryDataFile.validateFileSize(transactionData.getSize());

        // Set filePath to the location of the ArbitraryDataFile
        this.filePath = arbitraryDataFile.getFilePath();
    }

    private void decrypt() throws DataException {
        // Check if we can combine decrypt+unzip for better performance
        // This optimization eliminates an intermediate disk write/read cycle
        byte[] secret = this.secret58 != null ? Base58.decode(this.secret58) : null;
        Compression compression = transactionData != null ? transactionData.getCompression() : Compression.ZIP;
        
        if (secret != null && secret.length == Transformer.AES256_LENGTH && compression == Compression.ZIP) {
            // Try combined decrypt+unzip first (optimization)
            try {
                this.decryptAndUnzipCombined("AES/CBC/PKCS5Padding");
                this.decryptAndUnzipCombined = true;
                return;
            } catch (DataException e) {
                LOGGER.debug("Combined decrypt+unzip failed with AES/CBC/PKCS5Padding, trying fallback: {}", e.getMessage());
                try {
                    // Fall back to legacy AES algorithm
                    this.decryptAndUnzipCombined("AES");
                    this.decryptAndUnzipCombined = true;
                    return;
                } catch (DataException e2) {
                    LOGGER.info("Combined decrypt+unzip failed, falling back to separate operations: {}", e2.getMessage());
                    // Fall through to separate decrypt/uncompress
                }
            }
        }
        
        // Standard separate decrypt (for unencrypted, NONE compression, or if combined failed)
        try {
            // First try with explicit parameters (CBC mode with PKCS5 padding)
            this.decryptUsingAlgo("AES/CBC/PKCS5Padding");

        } catch (DataException e) {
            LOGGER.info("Unable to decrypt using specific parameters: {}", e.getMessage());
            // Something went wrong, so fall back to default AES params (necessary for legacy resource support)
            this.decryptUsingAlgo("AES");

            // TODO: delete files and block this resource if privateDataEnabled is false and the second attempt fails too
        }
    }

    /**
     * Combined decrypt and unzip in a single streaming pass.
     * This optimization eliminates the intermediate decrypted file, reducing disk I/O by ~50%.
     * 
     * @param algorithm The encryption algorithm to use
     * @throws DataException If decryption or extraction fails
     */
    private void decryptAndUnzipCombined(String algorithm) throws DataException {
        byte[] secret = this.secret58 != null ? Base58.decode(this.secret58) : null;
        if (secret == null || secret.length != Transformer.AES256_LENGTH) {
            throw new DataException("Invalid secret key for combined decrypt+unzip");
        }
        
        if (this.filePath == null || !Files.exists(this.filePath)) {
            throw new DataException("Can't decrypt+unzip non-existent file path");
        }
        
        try {
            LOGGER.debug("Decrypting and unzipping {} in single pass using algorithm {}...", this.arbitraryDataResource, algorithm);
            SecretKey aesKey = new SecretKeySpec(secret, 0, secret.length, "AES");
            
            // Use combined decrypt+unzip which streams directly to destination
            ZipUtils.decryptAndUnzip(algorithm, aesKey, this.filePath.toString(), 
                    this.uncompressedPath.getParent().toString());
            
            LOGGER.debug("Finished decrypting and unzipping {} using algorithm {}", this.arbitraryDataResource, algorithm);
            
            // Verify the uncompressed directory was created
            if (!this.uncompressedPath.toFile().exists()) {
                throw new DataException(String.format("Uncompressed directory not created at %s", this.uncompressedPath));
            }
            
            // Update filePath to point to uncompressed directory
            // Don't delete the original ArbitraryDataFile chunk - it needs to be preserved for peer sharing
            this.filePath = this.uncompressedPath;
            
        } catch (NoSuchAlgorithmException | InvalidAlgorithmParameterException | NoSuchPaddingException
                | InvalidKeyException | IOException e) {
            LOGGER.info(String.format("Exception when decrypting+unzipping %s using algorithm %s", this.arbitraryDataResource, algorithm), e);
            throw new DataException(String.format("Unable to decrypt+unzip file at path %s using algorithm %s: %s", 
                    this.filePath, algorithm, e.getMessage()));
        }
    }

    private void decryptUsingAlgo(String algorithm) throws DataException {
        // Decrypt if we have the secret key.
        byte[] secret = this.secret58 != null ? Base58.decode(this.secret58) : null;
        if (secret != null && secret.length == Transformer.AES256_LENGTH) {
            try {
                LOGGER.debug("Decrypting {} using algorithm {}...", this.arbitraryDataResource, algorithm);
                Path unencryptedPath = Paths.get(this.workingPath.toString(), "zipped.zip");
                SecretKey aesKey = new SecretKeySpec(secret, 0, secret.length, "AES");
                AES.decryptFile(algorithm, aesKey, this.filePath.toString(), unencryptedPath.toString());
                LOGGER.debug("Finished decrypting {} using algorithm {}", this.arbitraryDataResource, algorithm);

                // Replace filePath pointer with the encrypted file path
                // Don't delete the original ArbitraryDataFile, as this is handled in the cleanup phase
                this.filePath = unencryptedPath;

            } catch (NoSuchAlgorithmException | InvalidAlgorithmParameterException | NoSuchPaddingException
                    | BadPaddingException | IllegalBlockSizeException | IOException | InvalidKeyException e) {
                LOGGER.info(String.format("Exception when decrypting %s using algorithm %s", this.arbitraryDataResource, algorithm), e);
                throw new DataException(String.format("Unable to decrypt file at path %s using algorithm %s: %s", this.filePath, algorithm, e.getMessage()));
            }
        } else {
            // Assume it is unencrypted. This will be the case when we have built a custom path by combining
            // multiple decrypted archives into a single state.
        }
    }

    private void uncompress() throws IOException, DataException {
        // Skip if we already did combined decrypt+unzip
        if (this.decryptAndUnzipCombined) {
            return;
        }
        
        if (this.filePath == null || !Files.exists(this.filePath)) {
            throw new DataException("Can't uncompress non-existent file path");
        }
        File file = new File(this.filePath.toString());
        if (file.isDirectory()) {
            // Already a directory - nothing to uncompress
            // We still need to copy the directory to its final destination if it's not already there
            this.moveFilePathToFinalDestination();
            return;
        }

        try {
            // Default to ZIP compression - this is needed for previews
            Compression compression = transactionData != null ? transactionData.getCompression() : Compression.ZIP;

            // Handle each type of compression
            if (compression == Compression.ZIP) {
                LOGGER.debug("Unzipping {}...", this.arbitraryDataResource);
                ZipUtils.unzip(this.filePath.toString(), this.uncompressedPath.getParent().toString());
                LOGGER.debug("Finished unzipping {}", this.arbitraryDataResource);
            }
            else if (compression == Compression.NONE) {
                Files.createDirectories(this.uncompressedPath);
                Path finalPath = Paths.get(this.uncompressedPath.toString(), "data");
                this.filePath.toFile().renameTo(finalPath.toFile());
            }
            else {
                throw new DataException(String.format("Unrecognized compression type: %s", transactionData.getCompression()));
            }
        } catch (IOException e) {
            throw new DataException(String.format("Unable to unzip file: %s", e.getMessage()));
        }

        if (!this.uncompressedPath.toFile().exists()) {
            throw new DataException(String.format("Unable to unzip file: %s", this.filePath));
        }

        // Update filePath to point to uncompressed directory
        // Don't delete the original ArbitraryDataFile chunk - it needs to be preserved for peer sharing
        this.filePath = this.uncompressedPath;
    }

    private void validate() throws IOException, DataException {
        if (this.service.isValidationRequired()) {
            LOGGER.debug("Validating {}...", this.arbitraryDataResource);
            Service.ValidationResult result = this.service.validate(this.filePath);
            if (result != Service.ValidationResult.OK) {
                throw new DataException(String.format("Validation of %s failed: %s", this.service, result.toString()));
            }
            LOGGER.debug("Finished validating {}", this.arbitraryDataResource);
        }
    }


    private void moveFilePathToFinalDestination() throws IOException, DataException {
        if (this.filePath.compareTo(this.uncompressedPath) != 0) {
            File source = new File(this.filePath.toString());
            File dest = new File(this.uncompressedPath.toString());
            if (!source.exists()) {
                throw new DataException("Source directory doesn't exist");
            }
            // Ensure destination directory doesn't exist
            FileUtils.deleteDirectory(dest);
            // These are completed temporary builds. Rename on the same filesystem so large
            // resources do not incur another full read/write pass before they can be served.
            if (FilesystemUtils.pathInsideDataOrTempPath(this.filePath)) {
                moveBuiltDirectory(source.toPath(), dest.toPath());
            } else {
                // Preserve externally supplied sources. Unlike the old helper, copy errors propagate.
                FileUtils.copyDirectory(source, dest);
            }

            try {
                // ... and its parent directory if empty
                Path parentDirectory = this.filePath.getParent();
                if (FilesystemUtils.pathInsideDataOrTempPath(parentDirectory)) {
                    Files.deleteIfExists(parentDirectory);
                }

            } catch (DirectoryNotEmptyException e) {
                    // No need to log anything
            } catch (IOException e) {
                // This will eventually be cleaned up by a maintenance process, so log the error and continue
                LOGGER.debug("Unable to cleanup directories: {}", e.getMessage());
            }

            // Finally, update filePath to point to uncompressedPath
            this.filePath = this.uncompressedPath;
        }
    }

    /** Publish a completed directory; a failed fallback copy must leave the source intact. */
    static void moveBuiltDirectory(Path source, Path destination) throws IOException {
        if (Files.exists(destination))
            throw new FileAlreadyExistsException(destination.toString());
        Files.createDirectories(destination.getParent());
        try {
            Files.move(source, destination, StandardCopyOption.ATOMIC_MOVE);
            return;
        } catch (AtomicMoveNotSupportedException e) {
            // Different filesystems (or a provider without atomic renames): copy to a private
            // staging directory first so an incomplete tree is never published as the result.
        }

        Path staging = Files.createTempDirectory(destination.getParent(), ".qdn-build-");
        try {
            FileUtils.copyDirectory(source.toFile(), staging.toFile());
            try {
                Files.move(staging, destination, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(staging, destination);
            }
        } finally {
            if (Files.exists(staging))
                FileUtils.deleteDirectory(staging.toFile());
        }

        // Publication succeeded. A leftover source can be cleaned later without discarding
        // the successfully built destination.
        try {
            FileUtils.deleteDirectory(source.toFile());
        } catch (IOException e) {
            LOGGER.debug("Unable to remove source directory after publishing build: {}", e.getMessage());
        }
    }


    public void setTransactionData(ArbitraryTransactionData transactionData) {
        this.transactionData = transactionData;
    }

    public void setSecret58(String secret58) {
        this.secret58 = secret58;
    }

    public Path getFilePath() {
        return this.filePath;
    }

    public Path getUncompressedPath() {
        return this.uncompressedPath;
    }

    public void setFilePath(Path filePath) {
        this.filePath = filePath;
    }

    public int getLayerCount() {
        return this.layerCount;
    }

    public byte[] getLatestSignature() {
        return this.latestSignature;
    }

    /**
     * Use the below setter to ensure that we only read existing
     * data without requesting any missing files,
     *
     * @param canRequestMissingFiles - whether or not fetching missing files is allowed
     */
    public void setCanRequestMissingFiles(boolean canRequestMissingFiles) {
        this.canRequestMissingFiles = canRequestMissingFiles;
    }

}
