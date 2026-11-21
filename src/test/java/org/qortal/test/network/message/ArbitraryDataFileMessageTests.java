package org.qortal.test.network.message;

import org.junit.Before;
import org.junit.Test;
import org.qortal.arbitrary.ArbitraryDataFile;
import org.qortal.arbitrary.QdnByteArrayPool;
import org.qortal.network.message.ArbitraryDataFileMessage;
import org.qortal.network.message.Message;
import org.qortal.repository.DataException;
import org.qortal.test.common.Common;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertSame;

public class ArbitraryDataFileMessageTests extends Common {

    @Before
    public void beforeTest() throws Exception {
        Common.useDefaultSettings();
        QdnByteArrayPool.clearForTesting();
    }

    @Test
    public void standardIncomingChunkReturnsItsBufferToBoundedPool() throws Exception {
        byte[] signature = bytes(64, 17);
        byte[] data = bytes(ArbitraryDataFile.CHUNK_SIZE, 29);
        ByteBuffer payload = ByteBuffer.allocate(signature.length + Integer.BYTES + data.length);
        payload.put(signature).putInt(data.length).put(data).flip();

        ArbitraryDataFileMessage message = (ArbitraryDataFileMessage)
                ArbitraryDataFileMessage.fromByteBuffer(77, payload);
        byte[] receivedBuffer = message.getArbitraryDataFile().getBytes();
        assertArrayEquals(data, receivedBuffer);

        message.getArbitraryDataFile().clearFileContent();
        assertSame(receivedBuffer, QdnByteArrayPool.acquire(ArbitraryDataFile.CHUNK_SIZE));
    }

    @Test
    public void exactBufferSerializationRoundTrips() throws Exception {
        byte[] signature = bytes(64, 11);
        byte[] data = bytes(128 * 1024, 23);
        ArbitraryDataFile file = new ArbitraryDataFile(data, signature, false);
        ArbitraryDataFileMessage outgoing = new ArbitraryDataFileMessage(signature, file);
        outgoing.setId(1234);

        assertFalse("In-memory relay data does not need a disk prefetch", outgoing.startPrefetch());
        Message decoded = Message.fromByteBuffer(ByteBuffer.wrap(outgoing.toBytes()).asReadOnlyBuffer());

        assertTrue(decoded instanceof ArbitraryDataFileMessage);
        ArbitraryDataFileMessage incoming = (ArbitraryDataFileMessage) decoded;
        assertEquals(1234, incoming.getId());
        assertArrayEquals(signature, incoming.getSignature());
        assertArrayEquals(data, incoming.getArbitraryDataFile().getBytes());
    }

    @Test
    public void queuedPrefetchAndSerializationShareOneRead() throws Exception {
        byte[] signature = bytes(64, 31);
        byte[] data = bytes(256 * 1024, 47);
        CountingFile file = new CountingFile(data, signature);
        ArbitraryDataFileMessage outgoing = new ArbitraryDataFileMessage(signature, file);
        outgoing.setId(5678);

        assertTrue(outgoing.startPrefetch());
        Message decoded = Message.fromByteBuffer(ByteBuffer.wrap(outgoing.toBytes()).asReadOnlyBuffer());

        assertEquals(1, file.readCount.get());
        assertTrue(decoded instanceof ArbitraryDataFileMessage);
        assertArrayEquals(data, ((ArbitraryDataFileMessage) decoded).getArbitraryDataFile().getBytes());
    }

    @Test
    public void failedPrefetchFallsBackToSynchronousRead() throws Exception {
        byte[] signature = bytes(64, 53);
        byte[] data = bytes(64 * 1024, 71);
        RetryFile file = new RetryFile(data, signature);
        ArbitraryDataFileMessage outgoing = new ArbitraryDataFileMessage(signature, file);
        outgoing.setId(9012);

        assertTrue(outgoing.startPrefetch());
        Message decoded = Message.fromByteBuffer(ByteBuffer.wrap(outgoing.toBytes()).asReadOnlyBuffer());

        assertEquals(2, file.readCount.get());
        assertArrayEquals(data, ((ArbitraryDataFileMessage) decoded).getArbitraryDataFile().getBytes());
    }

    private static byte[] bytes(int size, int seed) {
        byte[] value = new byte[size];
        for (int i = 0; i < value.length; ++i)
            value[i] = (byte) (seed + i * 13);
        return value;
    }

    private static final class CountingFile extends ArbitraryDataFile {
        private final byte[] data;
        private final AtomicInteger readCount = new AtomicInteger();

        private CountingFile(byte[] data, byte[] signature) throws DataException {
            super(data, signature, false);
            this.data = Arrays.copyOf(data, data.length);
            this.clearFileContent();
        }

        @Override
        public byte[] getBytes() {
            this.readCount.incrementAndGet();
            try {
                Thread.sleep(25L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return this.data;
        }
    }

    private static final class RetryFile extends ArbitraryDataFile {
        private final byte[] data;
        private final AtomicInteger readCount = new AtomicInteger();

        private RetryFile(byte[] data, byte[] signature) throws DataException {
            super(data, signature, false);
            this.data = Arrays.copyOf(data, data.length);
            this.clearFileContent();
        }

        @Override
        public byte[] getBytes() {
            return this.readCount.incrementAndGet() == 1 ? null : this.data;
        }
    }
}
