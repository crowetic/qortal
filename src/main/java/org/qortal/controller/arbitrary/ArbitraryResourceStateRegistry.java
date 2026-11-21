package org.qortal.controller.arbitrary;

import org.qortal.arbitrary.misc.Service;
import org.qortal.data.arbitrary.ArbitraryResourceStatus;
import org.qortal.utils.ByteArray;

import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Bounded, process-local snapshots of QDN resource state.
 *
 * Repository and filesystem data remain authoritative. Mutation events invalidate a
 * snapshot immediately, while a short maximum age catches external filesystem changes.
 * A single in-flight calculation per resource coalesces concurrent status requests.
 */
public final class ArbitraryResourceStateRegistry {

    private static final int MAX_ENTRIES = 16_384;
    private static final long MAX_SNAPSHOT_AGE = 30_000L;
    private static final ArbitraryResourceStateRegistry INSTANCE = new ArbitraryResourceStateRegistry();

    private final ConcurrentHashMap<ResourceKey, Entry> entries = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ByteArray, Set<ResourceKey>> keysBySignature = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<ResourceKey> insertionOrder = new ConcurrentLinkedQueue<>();

    private ArbitraryResourceStateRegistry() {
    }

    public static ArbitraryResourceStateRegistry getInstance() {
        return INSTANCE;
    }

    public void clearForTesting() {
        this.entries.clear();
        this.keysBySignature.clear();
        this.insertionOrder.clear();
    }

    public ArbitraryResourceStatus getOrReconcile(Service service, String name, String identifier,
                                                   byte[] expectedSignature,
                                                   Reconciliation reconciliation) throws Exception {
        ResourceKey key = ResourceKey.of(service, name, identifier);
        if (key == null)
            return reconciliation.calculate();

        Entry entry = this.entries.get(key);
        if (entry == null) {
            Entry newEntry = new Entry();
            Entry existing = this.entries.putIfAbsent(key, newEntry);
            entry = existing != null ? existing : newEntry;
            if (existing == null) {
                this.insertionOrder.offer(key);
                trimIfNeeded();
            }
        }

        while (true) {
            Snapshot snapshot = entry.snapshot;
            long now = System.currentTimeMillis();
            if (!entry.dirty && snapshot != null && now - snapshot.confirmedAt <= MAX_SNAPSHOT_AGE
                    && signatureMatches(snapshot.signature, expectedSignature))
                return snapshot.status;

            CompletableFuture<Snapshot> future = entry.inFlight.get();
            if (future == null) {
                CompletableFuture<Snapshot> started = new CompletableFuture<>();
                if (entry.inFlight.compareAndSet(null, started)) {
                    long revision = entry.revision.get();
                    try {
                        ArbitraryResourceStatus status = reconciliation.calculate();
                        byte[] signature = expectedSignature == null ? null
                                : Arrays.copyOf(expectedSignature, expectedSignature.length);
                        Snapshot result = new Snapshot(status, signature, System.currentTimeMillis());
                        publish(key, entry, result, revision);
                        started.complete(result);
                        return status;
                    } catch (Throwable e) {
                        started.completeExceptionally(e);
                        if (e instanceof Exception)
                            throw (Exception) e;
                        throw e;
                    } finally {
                        entry.inFlight.compareAndSet(started, null);
                    }
                }
                continue;
            }

            Snapshot result;
            try {
                result = future.join();
            } catch (CompletionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof Exception)
                    throw (Exception) cause;
                throw e;
            }
            if (signatureMatches(result.signature, expectedSignature))
                return result.status;
        }
    }

    public void invalidate(Service service, String name, String identifier) {
        ResourceKey key = ResourceKey.of(service, name, identifier);
        if (key == null)
            return;
        Entry entry = this.entries.get(key);
        if (entry != null)
            invalidate(entry);
    }

    public void associateSignature(Service service, String name, String identifier, byte[] signature) {
        ResourceKey key = ResourceKey.of(service, name, identifier);
        if (key == null || signature == null)
            return;
        Entry entry = this.entries.get(key);
        if (entry == null)
            return;

        ByteArray previous;
        ByteArray current = ByteArray.copyOf(signature);
        synchronized (entry) {
            previous = entry.signature;
            entry.signature = current;
            Snapshot snapshot = entry.snapshot;
            if (snapshot != null)
                entry.snapshot = new Snapshot(snapshot.status, Arrays.copyOf(signature, signature.length),
                        snapshot.confirmedAt);
        }
        if (previous != null && !previous.equals(current))
            removeSignatureKey(previous, key);
        // The bounded registry can evict this entry while reconciliation is in progress.
        // Do not recreate a reverse-index entry for an object that is no longer active.
        if (this.entries.get(key) == entry)
            this.keysBySignature.computeIfAbsent(current, ignored -> ConcurrentHashMap.newKeySet()).add(key);
    }

    public void invalidateSignature(byte[] signature) {
        if (signature == null)
            return;
        Set<ResourceKey> keys = this.keysBySignature.get(ByteArray.wrap(signature));
        if (keys == null)
            return;
        for (ResourceKey key : keys) {
            Entry entry = this.entries.get(key);
            if (entry != null)
                invalidate(entry);
        }
    }

    private void invalidate(Entry entry) {
        entry.revision.incrementAndGet();
        entry.dirty = true;
    }

    private void publish(ResourceKey key, Entry entry, Snapshot snapshot, long reconciledRevision) {
        ByteArray previousSignature;
        ByteArray currentSignature;
        synchronized (entry) {
            previousSignature = entry.signature;
            entry.snapshot = snapshot;
            entry.signature = snapshot.signature == null ? null : ByteArray.copyOf(snapshot.signature);
            currentSignature = entry.signature;
            entry.dirty = entry.revision.get() != reconciledRevision;
        }

        if (previousSignature != null && !previousSignature.equals(currentSignature))
            removeSignatureKey(previousSignature, key);
        if (currentSignature != null && this.entries.get(key) == entry)
            this.keysBySignature.computeIfAbsent(currentSignature, ignored -> ConcurrentHashMap.newKeySet()).add(key);
    }

    private void trimIfNeeded() {
        while (this.entries.size() > MAX_ENTRIES) {
            ResourceKey oldest = this.insertionOrder.poll();
            if (oldest == null)
                return;
            Entry removed = this.entries.remove(oldest);
            if (removed != null && removed.signature != null)
                removeSignatureKey(removed.signature, oldest);
        }
    }

    private void removeSignatureKey(ByteArray signature, ResourceKey key) {
        this.keysBySignature.computeIfPresent(signature, (ignored, keys) -> {
            keys.remove(key);
            return keys.isEmpty() ? null : keys;
        });
    }

    private static boolean signatureMatches(byte[] actual, byte[] expected) {
        return expected == null || Arrays.equals(actual, expected);
    }

    @FunctionalInterface
    public interface Reconciliation {
        ArbitraryResourceStatus calculate() throws Exception;
    }

    private static final class Entry {
        private final AtomicLong revision = new AtomicLong();
        private final AtomicReference<CompletableFuture<Snapshot>> inFlight = new AtomicReference<>();
        private volatile Snapshot snapshot;
        private volatile ByteArray signature;
        private volatile boolean dirty = true;
    }

    private static final class Snapshot {
        private final ArbitraryResourceStatus status;
        private final byte[] signature;
        private final long confirmedAt;

        private Snapshot(ArbitraryResourceStatus status, byte[] signature, long confirmedAt) {
            this.status = status;
            this.signature = signature;
            this.confirmedAt = confirmedAt;
        }
    }

    private static final class ResourceKey {
        private final int service;
        private final String name;
        private final String identifier;
        private final int hashCode;

        private ResourceKey(Service service, String name, String identifier) {
            this.service = service.value;
            this.name = name.toLowerCase(Locale.ROOT);
            this.identifier = normalizeIdentifier(identifier);
            this.hashCode = Objects.hash(this.service, this.name, this.identifier);
        }

        private static ResourceKey of(Service service, String name, String identifier) {
            return service == null || name == null ? null : new ResourceKey(service, name, identifier);
        }

        private static String normalizeIdentifier(String identifier) {
            return identifier == null || identifier.isEmpty() || "default".equals(identifier)
                    ? "default" : identifier;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other)
                return true;
            if (!(other instanceof ResourceKey))
                return false;
            ResourceKey key = (ResourceKey) other;
            return this.service == key.service && this.name.equals(key.name)
                    && this.identifier.equals(key.identifier);
        }

        @Override
        public int hashCode() {
            return this.hashCode;
        }
    }
}
