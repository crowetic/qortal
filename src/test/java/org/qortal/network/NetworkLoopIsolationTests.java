package org.qortal.network;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.Test;
import org.qortal.test.common.Common;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class NetworkLoopIsolationTests extends Common {
    @Test
    public void blockchainSelectorSurvivesMalformedPeerAndReportsThreadLiveness() throws Exception {
        Common.useDefaultSettings();
        checkLoop(Network.getInstance());
    }

    @Test
    public void dataSelectorSurvivesMalformedPeerAndReportsThreadLiveness() throws Exception {
        Common.useDefaultSettings();
        checkLoop(NetworkData.getInstance());
    }

    private void checkLoop(Object network) throws Exception {
        Object priorSelector = FieldUtils.readField(network, "channelSelector", true);
        Object priorThread = FieldUtils.readField(network, "ioThread", true);
        boolean priorStopping = (boolean) FieldUtils.readField(network, "isShuttingDown", true);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch disconnected = new CountDownLatch(1);
        CountDownLatch healthyRead = new CountDownLatch(1);
        Thread loop = null;
        try (Selector selector = Selector.open(); Pair bad = new Pair(); Pair good = new Pair()) {
            Peer malformed = mock(Peer.class);
            Peer healthy = mock(Peer.class);
            doThrow(new IllegalArgumentException("malformed frame decoder")).when(malformed).readChannel();
            doAnswer(invocation -> {
                bad.receiver.close();
                disconnected.countDown();
                return null;
            }).when(malformed).disconnect(anyString());
            doAnswer(invocation -> {
                good.receiver.read(ByteBuffer.allocate(8));
                healthyRead.countDown();
                return null;
            }).when(healthy).readChannel();
            bad.receiver.register(selector, SelectionKey.OP_READ, malformed);
            good.receiver.register(selector, SelectionKey.OP_READ, healthy);
            FieldUtils.writeField(network, "channelSelector", selector, true);
            FieldUtils.writeField(network, "isShuttingDown", false, true);
            Method run = network.getClass().getDeclaredMethod("runIOLoop");
            run.setAccessible(true);
            loop = new Thread(() -> {
                try { run.invoke(network); } catch (Throwable e) { failure.set(e); }
            }, "test-selector");
            FieldUtils.writeField(network, "ioThread", loop, true);
            Method alive = network.getClass().getMethod("isIoThreadAlive");
            assertFalse((boolean) alive.invoke(network));
            loop.start();
            bad.sender.write(ByteBuffer.wrap(new byte[]{1}));
            assertTrue("Malformed peer was not disconnected", disconnected.await(5, TimeUnit.SECONDS));
            good.sender.write(ByteBuffer.wrap(new byte[]{2}));
            assertTrue("Healthy peer lost service", healthyRead.await(5, TimeUnit.SECONDS));
            assertTrue((boolean) alive.invoke(network));
            assertNull(failure.get());
            FieldUtils.writeField(network, "isShuttingDown", true, true);
            loop.interrupt();
            loop.join(5000);
            assertFalse((boolean) alive.invoke(network));
            assertNull(failure.get());
        } finally {
            FieldUtils.writeField(network, "isShuttingDown", true, true);
            if (loop != null) {
                loop.interrupt();
                loop.join(5000);
                assertFalse("Selector failed to stop", loop.isAlive());
            }
            FieldUtils.writeField(network, "channelSelector", priorSelector, true);
            FieldUtils.writeField(network, "ioThread", priorThread, true);
            FieldUtils.writeField(network, "isShuttingDown", priorStopping, true);
        }
    }

    private static class Pair implements AutoCloseable {
        final SocketChannel sender;
        final SocketChannel receiver;
        Pair() throws Exception {
            try (ServerSocketChannel listener = ServerSocketChannel.open()) {
                listener.bind(new InetSocketAddress("127.0.0.1", 0));
                sender = SocketChannel.open(listener.getLocalAddress());
                receiver = listener.accept();
                receiver.configureBlocking(false);
            }
        }
        public void close() throws Exception { sender.close(); receiver.close(); }
    }
}
