package org.qortal.network;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.Before;
import org.junit.Test;
import org.qortal.data.network.PeerData;
import org.qortal.network.message.Message;
import org.qortal.network.message.MessageException;
import org.qortal.network.message.MessageType;
import org.qortal.network.message.SignaturesMessage;
import org.qortal.test.common.Common;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

public class PeerBufferTests extends Common {
    @Before
    public void settings() throws Exception {
        Common.useDefaultSettings();
    }

    @Test
    public void fragmentedLargeFrameAndFollowingFrameSurviveBufferGrowth() throws Exception {
        try (Connection c = new Connection()) {
            List<byte[]> signatures = new ArrayList<>();
            for (int i = 0; i < 6000; i++) {
                byte[] signature = new byte[128];
                ByteBuffer.wrap(signature).putInt(i);
                signatures.add(signature);
            }
            byte[] frame = new SignaturesMessage(signatures).toBytes();
            c.send(frame, 0, 7); // Partial wire header
            c.peer.readChannel();
            assertEquals(Peer.RECEIVE_BUFFER_SIZE, c.buffer().capacity());
            for (int offset = 7; offset < frame.length;) {
                int length = Math.min(3179, frame.length - offset);
                c.send(frame, offset, length);
                c.peer.readChannel();
                offset += length;
            }
            c.send(new SignaturesMessage(Collections.emptyList()).toBytes());
            c.peer.readChannel();
            SignaturesMessage decoded = (SignaturesMessage) c.pending().poll();
            assertNotNull(decoded);
            assertEquals(signatures.size(), decoded.getSignatures().size());
            for (int i = 0; i < signatures.size(); i++)
                assertArrayEquals(signatures.get(i), decoded.getSignatures().get(i));
            assertNotNull(c.pending().poll());
            assertEquals(Peer.RECEIVE_BUFFER_SIZE, c.buffer().capacity());
        }
    }

    @Test
    public void idleSmallTrafficReusesSmallBufferAndDisconnectReleasesIt() throws Exception {
        try (Connection c = new Connection()) {
            byte[] frame = new SignaturesMessage(Collections.emptyList()).toBytes();
            c.send(frame);
            c.peer.readChannel();
            ByteBuffer retained = c.buffer();
            for (int i = 0; i < 20; i++) {
                c.send(frame);
                c.peer.readChannel();
                assertSame(retained, c.buffer());
            }
            assertEquals(8192, retained.capacity());
            c.peer.shutdown();
            assertNull(c.buffer());
        }
    }

    @Test(timeout = 10000)
    public void oversizedIncompleteFrameFailsInsteadOfSpinning() throws Exception {
        try (Connection c = new Connection()) {
            ByteBuffer header = ByteBuffer.allocate(17);
            header.put(Network.getInstance().getMessageMagic()).putInt(MessageType.SIGNATURES.value)
                    .put((byte) 0).putInt(10 * 1024 * 1024).putInt(0);
            c.send(header.array());
            c.peer.readChannel();
            try {
                for (int i = 0; i < 600; i++) {
                    c.send(new byte[4096]);
                    c.peer.readChannel();
                }
                fail("Oversized incomplete frame was accepted");
            } catch (IOException expected) {
                assertTrue(expected.getMessage().contains("receive buffer limit"));
            }
        }
    }

    @Test
    public void negativeWirePayloadLengthIsRejected() throws Exception {
        ByteBuffer header = ByteBuffer.allocate(13);
        header.put(Network.getInstance().getMessageMagic()).putInt(MessageType.SIGNATURES.value)
                .put((byte) 0).putInt(-1).flip();
        try {
            Message.fromByteBuffer(header);
            fail("Negative payload length was accepted");
        } catch (MessageException expected) {
            assertTrue(expected.getMessage().contains("Invalid data length"));
        }
    }

    @Test
    public void slowPeerSerializedQueueIsByteBoundedAndShutdownReturnsBudget() throws Exception {
        AtomicLong global = (AtomicLong) FieldUtils.readStaticField(Peer.class, "globalQueuedSerializedBytes", true);
        long baseline = global.get();
        try (Connection c = new Connection()) {
            // No selector is needed: these tests drive the socket writer directly.
            c.peer.setPeerType(-1);
            byte[] frame = new byte[512 * 1024];
            int count = (int) (Peer.MAX_QUEUED_SERIALIZED_BYTES / frame.length);
            for (int i = 0; i < count; i++)
                assertTrue(c.peer.sendPreSerializedMessage(i + 1, MessageType.ARBITRARY_DATA_FILE, frame.clone(), 0));
            assertFalse(c.peer.sendPreSerializedMessage(100, MessageType.ARBITRARY_DATA_FILE, frame, 0));
            assertEquals(count, c.peer.getSendQueueSize());
            assertEquals(baseline + Peer.MAX_QUEUED_SERIALIZED_BYTES, global.get());
            c.peer.shutdown();
            c.peer.shutdown(); // Idempotent release
            assertEquals(0, c.peer.getSendQueueSize());
            assertEquals(baseline, global.get());
        }
    }

    @Test
    public void globalSerializedBudgetLimitsManySlowPeersAndCanBeReused() throws Exception {
        AtomicLong global = (AtomicLong) FieldUtils.readStaticField(Peer.class, "globalQueuedSerializedBytes", true);
        long baseline = global.get();
        List<Connection> connections = new ArrayList<>();
        try {
            int peers = (int) (Peer.MAX_GLOBAL_QUEUED_SERIALIZED_BYTES / Peer.MAX_QUEUED_SERIALIZED_BYTES) + 1;
            byte[] frame = new byte[(int) Peer.MAX_QUEUED_SERIALIZED_BYTES];
            for (int i = 0; i < peers; i++) {
                Connection c = new Connection();
                connections.add(c);
                c.peer.setPeerType(-1);
                boolean accepted = c.peer.sendPreSerializedMessage(i + 1, MessageType.ARBITRARY_DATA_FILE, frame.clone(), 0);
                if (i == peers - 1)
                    assertFalse(accepted);
                else
                    assertTrue(accepted);
                assertTrue(global.get() <= Peer.MAX_GLOBAL_QUEUED_SERIALIZED_BYTES);
            }
            connections.get(0).peer.shutdown();
            assertTrue(connections.get(peers - 1).peer.sendPreSerializedMessage(100, MessageType.ARBITRARY_DATA_FILE, frame, 0));
        } finally {
            for (Connection c : connections)
                c.close();
        }
        assertEquals(baseline, global.get());
    }

    @Test(timeout = 10000)
    public void drainingSerializedFramesPreservesWireBytesAndReturnsBudget() throws Exception {
        AtomicLong global = (AtomicLong) FieldUtils.readStaticField(Peer.class, "globalQueuedSerializedBytes", true);
        long baseline = global.get();
        try (Connection c = new Connection()) {
            c.peer.setPeerType(-1);
            c.sender.configureBlocking(false);
            byte[] frame = new SignaturesMessage(Collections.nCopies(4000, new byte[128])).toBytes();
            ByteArrayOutputStream expected = new ByteArrayOutputStream();
            for (int i = 0; i < 8; i++) {
                assertTrue(c.peer.sendPreSerializedMessage(i + 1, MessageType.SIGNATURES, frame, 0));
                expected.write(frame);
            }
            ByteArrayOutputStream received = new ByteArrayOutputStream();
            ByteBuffer buffer = ByteBuffer.allocate(64 * 1024);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (received.size() < expected.size() && System.nanoTime() < deadline) {
                c.peer.writeChannel();
                int read;
                while ((read = c.sender.read(buffer)) > 0) {
                    received.write(buffer.array(), 0, read);
                    buffer.clear();
                }
            }
            assertArrayEquals(expected.toByteArray(), received.toByteArray());
            assertEquals(0, c.peer.getSendQueueSize());
            assertEquals(baseline, global.get());
        }
    }

    @Test
    public void concurrentProducersAndDisconnectCannotLeakSerializedBudget() throws Exception {
        AtomicLong global = (AtomicLong) FieldUtils.readStaticField(Peer.class, "globalQueuedSerializedBytes", true);
        long baseline = global.get();
        ExecutorService producers = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch accepted = new CountDownLatch(1);
        List<Future<?>> tasks = new ArrayList<>();
        try (Connection c = new Connection()) {
            c.peer.setPeerType(-1);
            byte[] frame = new byte[64 * 1024];
            for (int i = 0; i < 8; i++) {
                tasks.add(producers.submit(() -> {
                    start.await();
                    for (int j = 0; j < 100; j++) {
                        try {
                            if (c.peer.sendPreSerializedMessage(j + 1, MessageType.ARBITRARY_DATA_FILE, frame, 0))
                                accepted.countDown();
                        } catch (IOException closedDuringShutdown) {
                            break;
                        }
                    }
                    return null;
                }));
            }
            start.countDown();
            assertTrue(accepted.await(5, TimeUnit.SECONDS));
            c.peer.shutdown();
            for (Future<?> task : tasks) task.get(5, TimeUnit.SECONDS);
            assertEquals(0, c.peer.getSendQueueSize());
            assertEquals(baseline, global.get());
        } finally {
            producers.shutdownNow();
        }
    }

    static class Connection implements AutoCloseable {
        final SocketChannel sender;
        final SocketChannel receiver;
        final Peer peer;

        Connection() throws Exception {
            try (ServerSocketChannel listener = ServerSocketChannel.open()) {
                listener.bind(new InetSocketAddress("127.0.0.1", 0));
                sender = SocketChannel.open(listener.getLocalAddress());
                receiver = listener.accept();
            }
            receiver.configureBlocking(false);
            peer = new Peer(new PeerData(PeerAddress.fromString("127.0.0.1:12392")), Peer.NETWORK);
            FieldUtils.writeField(peer, "socketChannel", receiver, true);
            FieldUtils.writeField(peer, "resolvedAddress", receiver.getRemoteAddress(), true);
        }

        void send(byte[] bytes) throws Exception { send(bytes, 0, bytes.length); }
        void send(byte[] bytes, int offset, int length) throws Exception {
            ByteBuffer b = ByteBuffer.wrap(bytes, offset, length);
            while (b.hasRemaining()) sender.write(b);
        }
        ByteBuffer buffer() throws Exception { return (ByteBuffer) FieldUtils.readField(peer, "byteBuffer", true); }
        @SuppressWarnings("unchecked")
        BlockingQueue<Message> pending() throws Exception {
            return (BlockingQueue<Message>) FieldUtils.readField(peer, "pendingMessages", true);
        }
        public void close() throws Exception {
            peer.shutdown();
            sender.close();
            receiver.close();
        }
    }
}
