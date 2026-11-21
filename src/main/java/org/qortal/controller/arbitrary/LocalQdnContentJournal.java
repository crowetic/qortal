package org.qortal.controller.arbitrary;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.qortal.settings.Settings;
import org.qortal.utils.Base58;
import org.qortal.utils.ByteArray;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

/**
 * Small, node-local journal of signatures with confirmed QDN content.
 *
 * The filesystem remains authoritative. File mutations only enqueue a signature for
 * reconciliation; entries are persisted after cleanup has resolved the transaction and
 * confirmed complete data or chunks. Corrupt or truncated journals safely fall back to
 * gradual discovery and never authorize deletion by themselves.
 */
public final class LocalQdnContentJournal {

    private static final Logger LOGGER = LogManager.getLogger(LocalQdnContentJournal.class);
    private static final byte[] MAGIC = new byte[] {'Q', 'D', 'N', 'C', 'I', '0', '0', '1'};
    private static final int SIGNATURE_LENGTH = 64;
    private static final int RECORD_BODY_LENGTH = 1 + SIGNATURE_LENGTH + Long.BYTES;
    private static final int RECORD_LENGTH = RECORD_BODY_LENGTH + Integer.BYTES;
    private static final byte PRESENT = 1;
    private static final byte ABSENT = 2;
    private static final byte DISCOVERY_COMPLETE = 3;
    private static final byte DIRTY = 4;
    private static final int COMPACT_AFTER_MUTATIONS = 4_096;
    private static final int LEASE_STRIPES = 1_024;

    private static LocalQdnContentJournal instance;

    private final Path journalPath;
    private final Map<ByteArray, byte[]> hostedSignatures = new ConcurrentHashMap<>();
    private final Map<ByteArray, byte[]> changedSignatures = new ConcurrentHashMap<>();
    private final AtomicInteger changedSweepCursor = new AtomicInteger();
    private final AtomicInteger sweepCursor = new AtomicInteger();
    private final AtomicLong generation = new AtomicLong();
    private volatile byte[][] hostedSnapshot = new byte[0][];
    private volatile long hostedSnapshotGeneration = -1L;
    private final ReentrantLock[] signatureLeases = new ReentrantLock[LEASE_STRIPES];
    private volatile boolean discoveryComplete;
    private int mutationsSinceCompaction;

    private LocalQdnContentJournal() {
        this.journalPath = Paths.get(Settings.getInstance().getDataPath(), ".qdn-content-journal-v1");
        for (int i = 0; i < this.signatureLeases.length; ++i)
            this.signatureLeases[i] = new ReentrantLock();
        load();
    }

    public static synchronized LocalQdnContentJournal getInstance() {
        if (instance == null)
            instance = new LocalQdnContentJournal();
        return instance;
    }

    public static synchronized void resetForTesting() {
        instance = null;
    }

    public boolean isDiscoveryComplete() {
        return this.discoveryComplete;
    }

    public int size() {
        return this.hostedSignatures.size();
    }

    public boolean containsSignature(byte[] signature) {
        return signature != null && signature.length == SIGNATURE_LENGTH
                && this.hostedSignatures.containsKey(ByteArray.wrap(signature));
    }

    public long getGeneration() {
        return this.generation.get();
    }

    public boolean ownsPath(Path path) {
        if (path == null)
            return false;
        Path normalized = path.toAbsolutePath().normalize();
        return normalized.equals(this.journalPath.toAbsolutePath().normalize())
                || normalized.equals(this.journalPath.resolveSibling(
                        this.journalPath.getFileName() + ".tmp").toAbsolutePath().normalize());
    }

    public SignatureLease acquireMutationLease(byte[] signature) {
        if (signature == null || signature.length != SIGNATURE_LENGTH)
            return SignatureLease.NO_OP;
        ReentrantLock lock = this.signatureLeases[leaseIndex(signature)];
        lock.lock();
        return new SignatureLease(lock);
    }

    public SignatureLease tryAcquireCleanupLease(byte[] signature) {
        if (signature == null || signature.length != SIGNATURE_LENGTH)
            return null;
        ReentrantLock lock = this.signatureLeases[leaseIndex(signature)];
        return lock.tryLock() ? new SignatureLease(lock) : null;
    }

    private int leaseIndex(byte[] signature) {
        return (Arrays.hashCode(signature) & Integer.MAX_VALUE) % this.signatureLeases.length;
    }

    public static final class SignatureLease implements AutoCloseable {
        private static final SignatureLease NO_OP = new SignatureLease(null);
        private final ReentrantLock lock;

        private SignatureLease(ReentrantLock lock) {
            this.lock = lock;
        }

        @Override
        public void close() {
            if (this.lock != null)
                this.lock.unlock();
        }
    }

    public List<byte[]> snapshotSignatures() {
        List<byte[]> signatures = new ArrayList<>(this.hostedSignatures.size());
        for (byte[] signature : this.hostedSignatures.values())
            signatures.add(Arrays.copyOf(signature, signature.length));
        return signatures;
    }

    public synchronized void recordFilesystemChange(byte[] signature) {
        if (signature == null || signature.length != SIGNATURE_LENGTH)
            return;
        ArbitraryResourceStateRegistry.getInstance().invalidateSignature(signature);
        byte[] copy = Arrays.copyOf(signature, signature.length);
        if (this.changedSignatures.putIfAbsent(ByteArray.wrap(copy), copy) == null)
            append(DIRTY, copy);
    }

    public void recordPathChange(Path path) {
        if (path == null)
            return;

        Path dataPath = Paths.get(Settings.getInstance().getDataPath()).toAbsolutePath().normalize();
        Path current = path.toAbsolutePath().normalize();
        while (current != null && current.startsWith(dataPath) && !current.equals(dataPath)) {
            Path fileName = current.getFileName();
            if (fileName != null && fileName.toString().length() > 32) {
                try {
                    byte[] signature = Base58.decode(fileName.toString());
                    if (signature.length == SIGNATURE_LENGTH) {
                        recordFilesystemChange(signature);
                        return;
                    }
                } catch (NumberFormatException e) {
                    // Not a signature directory; continue with its parent.
                }
            }
            current = current.getParent();
        }
    }

    public synchronized void confirmPresent(byte[] signature) {
        if (signature == null || signature.length != SIGNATURE_LENGTH)
            return;
        byte[] copy = Arrays.copyOf(signature, signature.length);
        ByteArray key = ByteArray.wrap(copy);
        boolean wasDirty = this.changedSignatures.remove(key) != null;
        if (this.hostedSignatures.putIfAbsent(key, copy) == null) {
            this.generation.incrementAndGet();
            append(PRESENT, copy);
        } else if (wasDirty)
            append(PRESENT, copy);
    }

    /**
     * Persist a discovery batch with one open/flush/fsync instead of one fsync per path.
     * Keys are the already validated Base58 directory names, so discovery does not encode
     * every signature a second time.
     */
    public synchronized int confirmPresentBatch(Map<String, byte[]> signaturesByEncodedValue) {
        if (signaturesByEncodedValue == null || signaturesByEncodedValue.isEmpty())
            return 0;

        Map<String, byte[]> records = new LinkedHashMap<>();
        int added = 0;
        for (Map.Entry<String, byte[]> entry : signaturesByEncodedValue.entrySet()) {
            String signature58 = entry.getKey();
            byte[] signature = entry.getValue();
            if (signature58 == null || signature == null || signature.length != SIGNATURE_LENGTH)
                continue;

            byte[] copy = Arrays.copyOf(signature, signature.length);
            ByteArray key = ByteArray.wrap(copy);
            boolean wasDirty = this.changedSignatures.remove(key) != null;
            if (this.hostedSignatures.putIfAbsent(key, copy) == null) {
                added++;
                records.put(signature58, copy);
            } else if (wasDirty) {
                records.put(signature58, copy);
            }
        }

        if (added > 0)
            this.generation.addAndGet(added);
        // Discovery records are unique and markDiscoveryComplete() compacts once at the end.
        // Rewriting the growing journal every 4,096 discoveries would create quadratic I/O.
        appendBatch(PRESENT, records.values(), false);
        return added;
    }

    public synchronized void confirmAbsent(byte[] signature) {
        if (signature == null || signature.length != SIGNATURE_LENGTH)
            return;
        ByteArray key = ByteArray.wrap(signature);
        boolean wasDirty = this.changedSignatures.remove(key) != null;
        if (this.hostedSignatures.remove(key) != null) {
            this.generation.incrementAndGet();
            append(ABSENT, signature);
        } else if (wasDirty)
            append(ABSENT, signature);
    }

    public synchronized void markDiscoveryComplete() {
        if (this.discoveryComplete)
            return;
        this.discoveryComplete = true;
        appendBatch(DISCOVERY_COMPLETE,
                Collections.singletonList(new byte[SIGNATURE_LENGTH]), false);
        compact();
        LOGGER.info("QDN local-content discovery complete with {} hosted signatures", this.hostedSignatures.size());
    }

    /** Changed signatures first, optionally followed by a rotating audit of hosted content. */
    public List<byte[]> nextCandidates(int limit, boolean includeAudit) {
        if (limit <= 0)
            return Collections.emptyList();

        List<byte[]> result = new ArrayList<>(limit);
        List<byte[]> changed = new ArrayList<>(this.changedSignatures.values());
        if (!changed.isEmpty()) {
            int start = Math.floorMod(this.changedSweepCursor.getAndAdd(limit), changed.size());
            for (int i = 0; i < changed.size() && result.size() < limit; ++i) {
                byte[] signature = changed.get((start + i) % changed.size());
                result.add(Arrays.copyOf(signature, signature.length));
            }
        }

        if (!includeAudit || result.size() >= limit || this.hostedSignatures.isEmpty())
            return result;

        byte[][] hosted = currentHostedSnapshot();
        if (hosted.length == 0)
            return result;
        int start = Math.floorMod(this.sweepCursor.getAndAdd(limit - result.size()), hosted.length);
        for (int i = 0; i < hosted.length && result.size() < limit; ++i) {
            byte[] signature = hosted[(start + i) % hosted.length];
            boolean alreadyIncluded = result.stream().anyMatch(existing -> Arrays.equals(existing, signature));
            if (!alreadyIncluded)
                result.add(Arrays.copyOf(signature, signature.length));
        }
        return result;
    }

    private byte[][] currentHostedSnapshot() {
        long currentGeneration = this.generation.get();
        byte[][] snapshot = this.hostedSnapshot;
        if (this.hostedSnapshotGeneration == currentGeneration)
            return snapshot;

        synchronized (this) {
            currentGeneration = this.generation.get();
            if (this.hostedSnapshotGeneration != currentGeneration) {
                this.hostedSnapshot = this.hostedSignatures.values().toArray(new byte[0][]);
                this.hostedSnapshotGeneration = currentGeneration;
            }
            return this.hostedSnapshot;
        }
    }

    public synchronized void compact() {
        try {
            Path parent = this.journalPath.getParent();
            if (parent != null)
                Files.createDirectories(parent);
            Path temporary = this.journalPath.resolveSibling(this.journalPath.getFileName() + ".tmp");
            try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(
                    temporary, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)))) {
                output.write(MAGIC);
                for (byte[] signature : this.hostedSignatures.values())
                    writeRecord(output, PRESENT, signature);
                if (this.discoveryComplete)
                    writeRecord(output, DISCOVERY_COMPLETE, new byte[SIGNATURE_LENGTH]);
                for (byte[] signature : this.changedSignatures.values())
                    writeRecord(output, DIRTY, signature);
            }
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                channel.force(true);
            }
            try {
                Files.move(temporary, this.journalPath, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temporary, this.journalPath, StandardCopyOption.REPLACE_EXISTING);
            }
            this.mutationsSinceCompaction = 0;
        } catch (IOException e) {
            LOGGER.warn("Unable to compact QDN local-content journal; continuing with filesystem authority", e);
        }
    }

    private void load() {
        if (!Files.exists(this.journalPath))
            return;

        boolean invalid = false;
        try {
            long size = Files.size(this.journalPath);
            invalid = size < MAGIC.length || (size - MAGIC.length) % RECORD_LENGTH != 0;
        } catch (IOException e) {
            invalid = true;
        }
        try (DataInputStream input = new DataInputStream(new BufferedInputStream(Files.newInputStream(this.journalPath)))) {
            byte[] magic = new byte[MAGIC.length];
            input.readFully(magic);
            if (!Arrays.equals(magic, MAGIC))
                throw new IOException("unsupported journal header");

            while (true) {
                byte[] body = new byte[RECORD_BODY_LENGTH];
                try {
                    input.readFully(body);
                } catch (EOFException e) {
                    break;
                }
                int storedCrc;
                try {
                    storedCrc = input.readInt();
                } catch (EOFException e) {
                    invalid = true;
                    break;
                }
                CRC32 crc32 = new CRC32();
                crc32.update(body);
                if ((int) crc32.getValue() != storedCrc) {
                    invalid = true;
                    break;
                }

                byte operation = body[0];
                byte[] signature = Arrays.copyOfRange(body, 1, 1 + SIGNATURE_LENGTH);
                ByteArray key = ByteArray.copyOf(signature);
                if (operation == PRESENT) {
                    this.hostedSignatures.put(key, key.value);
                    this.changedSignatures.remove(key);
                }
                else if (operation == ABSENT) {
                    this.hostedSignatures.remove(key);
                    this.changedSignatures.remove(key);
                }
                else if (operation == DISCOVERY_COMPLETE)
                    this.discoveryComplete = true;
                else if (operation == DIRTY)
                    this.changedSignatures.put(key, key.value);
                else {
                    invalid = true;
                    break;
                }
            }
        } catch (IOException | RuntimeException e) {
            LOGGER.warn("Unable to read QDN local-content journal; gradual discovery will repair it: {}", e.getMessage());
            invalid = true;
        }

        if (invalid) {
            // Keep positively decoded entries as hints, but require a complete discovery pass
            // before the legacy scan is retired.
            this.discoveryComplete = false;
            compact();
        }
        LOGGER.info("Loaded {} QDN local-content signatures (discovery complete: {})",
                this.hostedSignatures.size(), this.discoveryComplete);
    }

    private void append(byte operation, byte[] signature) {
        appendBatch(operation, Collections.singletonList(signature), true);
    }

    private void appendBatch(byte operation, Collection<byte[]> signatures, boolean compactIfNeeded) {
        if (signatures == null || signatures.isEmpty())
            return;

        try {
            Path parent = this.journalPath.getParent();
            if (parent != null)
                Files.createDirectories(parent);
            boolean newFile = !Files.exists(this.journalPath) || Files.size(this.journalPath) == 0;
            try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(
                    this.journalPath, StandardOpenOption.CREATE, StandardOpenOption.APPEND)))) {
                if (newFile)
                    output.write(MAGIC);
                for (byte[] signature : signatures)
                    writeRecord(output, operation, signature);
            }
            try (FileChannel channel = FileChannel.open(this.journalPath, StandardOpenOption.WRITE)) {
                channel.force(false);
            }
            this.mutationsSinceCompaction += signatures.size();
            if (compactIfNeeded && this.mutationsSinceCompaction >= COMPACT_AFTER_MUTATIONS)
                compact();
        } catch (IOException e) {
            LOGGER.warn("Unable to append QDN local-content journal; filesystem change remains reconstructible", e);
            this.discoveryComplete = false;
        }
    }

    private static void writeRecord(DataOutputStream output, byte operation, byte[] signature) throws IOException {
        byte[] normalized = signature != null && signature.length == SIGNATURE_LENGTH
                ? signature : new byte[SIGNATURE_LENGTH];
        ByteBuffer body = ByteBuffer.allocate(RECORD_BODY_LENGTH);
        body.put(operation);
        body.put(normalized);
        body.putLong(System.currentTimeMillis());
        byte[] bytes = body.array();
        CRC32 crc32 = new CRC32();
        crc32.update(bytes);
        output.write(bytes);
        output.writeInt((int) crc32.getValue());
    }
}
