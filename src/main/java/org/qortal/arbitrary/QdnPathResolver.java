package org.qortal.arbitrary;

import org.qortal.repository.DataException;
import org.qortal.settings.Settings;
import org.qortal.utils.Base58;
import org.qortal.utils.ByteArray;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Resolves content-addressed QDN paths without repeatedly encoding the same
 * transaction signatures and chunk hashes.
 *
 * Encodings are deterministic and independent of filesystem state, so cached
 * values cannot become stale when a resource is updated or deleted. The cache
 * is process-local and bounded; the filesystem remains authoritative.
 */
public final class QdnPathResolver {

    private static final int MAX_CACHED_ENCODINGS = 32_768;

    private static final ConcurrentHashMap<ByteArray, String> ENCODINGS = new ConcurrentHashMap<>();
    private static final ConcurrentLinkedQueue<ByteArray> INSERTION_ORDER = new ConcurrentLinkedQueue<>();

    private QdnPathResolver() {
    }

    public static String encode(byte[] value) {
        if (value == null)
            return null;

        String cached = ENCODINGS.get(ByteArray.wrap(value));
        if (cached != null)
            return cached;

        String encoded = Base58.encode(value);
        ByteArray key = ByteArray.wrap(Arrays.copyOf(value, value.length));
        String existing = ENCODINGS.putIfAbsent(key, encoded);
        if (existing != null)
            return existing;

        INSERTION_ORDER.offer(key);
        trimIfNeeded();
        return encoded;
    }

    public static Path resolve(String hash58, byte[] signature) throws DataException {
        if (hash58 == null)
            return null;

        if (signature == null) {
            if (hash58.length() < 4)
                throw new DataException("Invalid QDN content hash");
            return Paths.get(Settings.getInstance().getDataPath(), "_misc",
                    hash58.substring(0, 2).toLowerCase(),
                    hash58.substring(2, 4).toLowerCase(), hash58);
        }

        String signature58 = encode(signature);
        if (signature58.length() < 4)
            throw new DataException("Invalid QDN transaction signature");

        return Paths.get(Settings.getInstance().getDataPath(),
                signature58.substring(0, 2).toLowerCase(),
                signature58.substring(2, 4).toLowerCase(), signature58, hash58);
    }

    public static Path resolveTransactionDirectory(byte[] signature) {
        if (signature == null)
            return null;
        String signature58 = encode(signature);
        if (signature58.length() < 4)
            return null;
        return Paths.get(Settings.getInstance().getDataPath(),
                signature58.substring(0, 2).toLowerCase(),
                signature58.substring(2, 4).toLowerCase(), signature58);
    }

    private static void trimIfNeeded() {
        while (ENCODINGS.size() > MAX_CACHED_ENCODINGS) {
            ByteArray oldest = INSERTION_ORDER.poll();
            if (oldest == null)
                return;
            ENCODINGS.remove(oldest);
        }
    }
}
