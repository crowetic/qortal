package org.qortal.arbitrary;

import java.util.concurrent.ArrayBlockingQueue;

/** Bounded pool for the standard QDN chunk payload size. */
public final class QdnByteArrayPool {

    private static final int MAX_POOLED_BUFFERS = 32;
    private static final ArrayBlockingQueue<byte[]> STANDARD_CHUNKS =
            new ArrayBlockingQueue<>(MAX_POOLED_BUFFERS);

    private QdnByteArrayPool() {
    }

    public static byte[] acquire(int length) {
        if (length != ArbitraryDataFile.CHUNK_SIZE)
            return new byte[length];
        byte[] pooled = STANDARD_CHUNKS.poll();
        return pooled != null ? pooled : new byte[length];
    }

    public static void release(byte[] buffer, int length) {
        if (buffer == null || length != ArbitraryDataFile.CHUNK_SIZE
                || buffer.length != ArbitraryDataFile.CHUNK_SIZE)
            return;
        STANDARD_CHUNKS.offer(buffer);
    }

    public static void clearForTesting() {
        STANDARD_CHUNKS.clear();
    }
}
