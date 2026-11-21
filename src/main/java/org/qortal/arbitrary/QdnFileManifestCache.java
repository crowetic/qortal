package org.qortal.arbitrary;

import org.qortal.arbitrary.metadata.ArbitraryDataTransactionMetadata;
import org.qortal.data.transaction.ArbitraryTransactionData;
import org.qortal.repository.DataException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Bounded, process-local cache of immutable QDN transaction file layouts.
 *
 * Metadata and transaction signatures are content addressed, so the expected
 * file paths never change for a cache key. File presence is deliberately not
 * cached: callers still see downloads and deletions immediately.
 */
public final class QdnFileManifestCache {

    private static final int MAX_MANIFESTS = 4_096;
    private static final int MAX_CHUNK_PATHS = 65_536;

    private static final ConcurrentHashMap<Key, CompletableFuture<Manifest>> CACHE = new ConcurrentHashMap<>();
    private static final ConcurrentLinkedQueue<Key> INSERTION_ORDER = new ConcurrentLinkedQueue<>();
    private static final AtomicInteger CACHED_CHUNK_PATHS = new AtomicInteger();

    private QdnFileManifestCache() {
    }

    public static Manifest get(ArbitraryTransactionData transactionData) throws DataException {
        if (transactionData == null || transactionData.getDataType() != ArbitraryTransactionData.DataType.DATA_HASH
                || transactionData.getMetadataHash() == null)
            return null;

        byte[] signature = transactionData.getSignature();
        byte[] metadataHash = transactionData.getMetadataHash();
        byte[] dataHash = transactionData.getData();
        Key lookupKey = new Key(signature, metadataHash, dataHash, false);

        CompletableFuture<Manifest> existing = CACHE.get(lookupKey);
        if (existing != null)
            return await(existing);

        Key storedKey = new Key(signature, metadataHash, dataHash, true);
        CompletableFuture<Manifest> loading = new CompletableFuture<>();
        existing = CACHE.putIfAbsent(storedKey, loading);
        if (existing != null)
            return await(existing);

        try {
            Manifest manifest = load(storedKey);
            if (manifest == null) {
                CACHE.remove(storedKey, loading);
                loading.complete(null);
                return null;
            }

            loading.complete(manifest);

            // Very large resources are still served, but are not allowed to displace the
            // entire cache or permanently exceed its memory bound.
            if (manifest.chunkCount() > MAX_CHUNK_PATHS) {
                CACHE.remove(storedKey, loading);
                return manifest;
            }

            CACHED_CHUNK_PATHS.addAndGet(manifest.chunkCount());
            INSERTION_ORDER.offer(storedKey);
            trimIfNeeded();
            return manifest;
        } catch (DataException e) {
            CACHE.remove(storedKey, loading);
            loading.completeExceptionally(e);
            throw e;
        } catch (RuntimeException e) {
            CACHE.remove(storedKey, loading);
            loading.completeExceptionally(e);
            throw e;
        }
    }

    private static Manifest load(Key key) throws DataException {
        Path dataPath = QdnPathResolver.resolve(QdnPathResolver.encode(key.dataHash), key.signature);
        Path metadataPath = QdnPathResolver.resolve(QdnPathResolver.encode(key.metadataHash), key.signature);
        if (!Files.exists(metadataPath))
            return null;

        try {
            ArbitraryDataTransactionMetadata metadata = new ArbitraryDataTransactionMetadata(metadataPath);
            metadata.read();
            List<byte[]> hashes = metadata.getChunks();
            if (hashes == null)
                hashes = Collections.emptyList();

            List<Path> chunkPaths = new ArrayList<>(hashes.size());
            for (byte[] hash : hashes)
                chunkPaths.add(QdnPathResolver.resolve(QdnPathResolver.encode(hash), key.signature));
            return new Manifest(dataPath, metadataPath, chunkPaths);
        } catch (IOException e) {
            throw new DataException(e);
        }
    }

    private static Manifest await(CompletableFuture<Manifest> future) throws DataException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DataException(e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof DataException)
                throw (DataException) cause;
            if (cause instanceof RuntimeException)
                throw (RuntimeException) cause;
            throw new DataException(cause);
        }
    }

    private static void trimIfNeeded() {
        while (CACHE.size() > MAX_MANIFESTS || CACHED_CHUNK_PATHS.get() > MAX_CHUNK_PATHS) {
            Key oldest = INSERTION_ORDER.poll();
            if (oldest == null)
                return;

            CompletableFuture<Manifest> removed = CACHE.remove(oldest);
            if (removed != null) {
                Manifest manifest = removed.getNow(null);
                if (manifest != null)
                    CACHED_CHUNK_PATHS.addAndGet(-manifest.chunkCount());
            }
        }
    }

    public static final class Manifest {
        private final Path dataPath;
        private final Path metadataPath;
        private final List<Path> chunkPaths;

        private Manifest(Path dataPath, Path metadataPath, List<Path> chunkPaths) {
            this.dataPath = dataPath;
            this.metadataPath = metadataPath;
            this.chunkPaths = Collections.unmodifiableList(chunkPaths);
        }

        public Path dataPath() {
            return this.dataPath;
        }

        public Path metadataPath() {
            return this.metadataPath;
        }

        public int chunkCount() {
            return this.chunkPaths.size();
        }

        public int expectedFileCount() {
            return Math.max(1, this.chunkPaths.size()) + 1;
        }

        public boolean completeFileExists() {
            return Files.exists(this.dataPath);
        }

        public boolean anyChunksExist() {
            for (Path chunkPath : this.chunkPaths) {
                if (Files.exists(chunkPath))
                    return true;
            }
            return false;
        }

        public boolean allChunksExist() {
            if (this.chunkPaths.isEmpty())
                return false;
            for (Path chunkPath : this.chunkPaths) {
                if (!Files.exists(chunkPath))
                    return false;
            }
            return true;
        }

        public boolean allFilesExist() {
            if (Files.exists(this.dataPath))
                return true;
            if (this.chunkPaths.isEmpty() || !Files.exists(this.metadataPath))
                return false;
            return allChunksExist();
        }
    }

    private static final class Key {
        private final byte[] signature;
        private final byte[] metadataHash;
        private final byte[] dataHash;
        private final int hashCode;

        private Key(byte[] signature, byte[] metadataHash, byte[] dataHash, boolean copy) {
            this.signature = copy ? Arrays.copyOf(signature, signature.length) : signature;
            this.metadataHash = copy ? Arrays.copyOf(metadataHash, metadataHash.length) : metadataHash;
            this.dataHash = copy ? Arrays.copyOf(dataHash, dataHash.length) : dataHash;
            int hash = Arrays.hashCode(this.signature);
            hash = 31 * hash + Arrays.hashCode(this.metadataHash);
            this.hashCode = 31 * hash + Arrays.hashCode(this.dataHash);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other)
                return true;
            if (!(other instanceof Key))
                return false;
            Key key = (Key) other;
            return Arrays.equals(this.signature, key.signature)
                    && Arrays.equals(this.metadataHash, key.metadataHash)
                    && Arrays.equals(this.dataHash, key.dataHash);
        }

        @Override
        public int hashCode() {
            return this.hashCode;
        }
    }

}
