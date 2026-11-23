package org.qortal.network;

import org.junit.Test;
import org.qortal.network.message.MessageException;
import org.qortal.network.message.Message;
import org.qortal.test.common.Common;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class PeerSendManagerBoundsTests extends Common {
    @Test
    public void blockedDiskReaderCannotAccumulateUnlimitedFactories() throws Exception {
        Common.useDefaultSettings();
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (PeerBufferTests.Connection c = new PeerBufferTests.Connection()) {
            PeerSendManager manager = new PeerSendManager(c.peer, false);
            try {
                manager.queueMessageFactory(() -> {
                    reading.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return null;
                }, 1);
                assertTrue(reading.await(5, TimeUnit.SECONDS));
                Message lazy = mock(Message.class);
                when(lazy.toBytes()).thenThrow(new AssertionError("Caller serialized a lazy message"));
                manager.queueMessage(lazy);
                verify(lazy, never()).toBytes();
                for (int i = 1; i < PeerSendManager.MAX_PENDING_MESSAGES; i++)
                    manager.queueMessageFactory(() -> null, 1);
                try {
                    manager.queueMessageFactory(() -> null, 1, "rejected");
                    fail("Unbounded priority queue accepted another factory");
                } catch (MessageException expected) {
                    assertFalse(manager.isHashQueued("rejected"));
                }
                assertEquals(PeerSendManager.MAX_PENDING_MESSAGES, manager.getQueueMessageSize());
                manager.shutdown();
                try {
                    manager.queueMessageFactory(() -> null, 1);
                    fail("Stopped manager accepted work");
                } catch (MessageException expected) {
                    assertEquals(0, manager.getQueueMessageSize());
                }
            } finally {
                manager.shutdown();
                release.countDown();
            }
        }
    }
}
