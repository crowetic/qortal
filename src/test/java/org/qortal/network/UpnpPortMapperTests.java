package org.qortal.network;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class UpnpPortMapperTests {
    private static final int P2P = 12392;
    private static final int QDN = 12394;
    private static final String ADDRESS = "198.51.100.5";

    private static class Gateway extends UpnpPortMapper.Gateway {
        final List<Integer> opened = new ArrayList<>();
        final List<Integer> closed = new ArrayList<>();

        @Override boolean available() { return true; }
        @Override boolean isMapped(int port) { return false; }
        @Override boolean open(int port) { opened.add(port); return true; }
        @Override boolean close(int port) { closed.add(port); return true; }
        @Override String mappedAddress(int port) { return ADDRESS; }
    }

    @Test(timeout = 10000)
    public void delayedP2pDoesNotBlockStartupAndBothPortsAreMappedInOrder() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Gateway gateway = new Gateway() {
            @Override boolean open(int port) {
                if (port == P2P) {
                    entered.countDown();
                    try {
                        if (!release.await(3, TimeUnit.SECONDS)) return false;
                    } catch (InterruptedException e) {
                        return false;
                    }
                }
                return super.open(port);
            }
        };
        UpnpPortMapper mapper = new UpnpPortMapper(gateway);
        List<String> addresses = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch published = new CountDownLatch(1);
        try {
            mapper.configure(P2P, true, addresses::add);
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            mapper.configure(QDN, true, address -> { addresses.add(address); published.countDown(); });
            assertTrue(addresses.isEmpty());
            release.countDown();
            assertTrue(published.await(2, TimeUnit.SECONDS));
        } finally {
            release.countDown();
            mapper.shutdown();
        }
        assertEquals(Arrays.asList(ADDRESS, ADDRESS), addresses);
        assertEquals(Arrays.asList(P2P, QDN), gateway.opened);
        assertEquals(gateway.opened, gateway.closed);
    }

    @Test(timeout = 12000)
    public void stopCleansUpAnUnacknowledgedAddAndSkipsQueuedPort() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        Gateway gateway = new Gateway() {
            @Override boolean open(int port) {
                super.open(port);
                entered.countDown();
                try {
                    new CountDownLatch(1).await();
                    return true;
                } catch (InterruptedException e) {
                    // The gateway accepted the add, but the HTTP client did not receive the reply.
                    return false;
                }
            }
        };
        UpnpPortMapper mapper = new UpnpPortMapper(gateway);
        List<String> addresses = new ArrayList<>();
        mapper.configure(P2P, true, addresses::add);
        mapper.configure(QDN, true, addresses::add);
        assertTrue(entered.await(2, TimeUnit.SECONDS));
        mapper.shutdown();
        mapper.configure(QDN, true, addresses::add);
        mapper.shutdown();
        assertEquals(Collections.singletonList(P2P), gateway.opened);
        assertEquals(Collections.singletonList(P2P), gateway.closed);
        assertTrue(addresses.isEmpty());
    }

    @Test(timeout = 12000)
    public void lostAddReplyDoesNotRemoveAPreexistingMapping() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        Gateway gateway = new Gateway() {
            @Override boolean isMapped(int port) { return true; }
            @Override boolean open(int port) {
                entered.countDown();
                try {
                    new CountDownLatch(1).await();
                    return true;
                } catch (InterruptedException e) {
                    return false;
                }
            }
        };
        UpnpPortMapper mapper = new UpnpPortMapper(gateway);
        List<String> addresses = new ArrayList<>();
        mapper.configure(P2P, true, addresses::add);
        assertTrue(entered.await(2, TimeUnit.SECONDS));
        mapper.shutdown();
        assertTrue(gateway.closed.isEmpty());
        assertTrue(addresses.isEmpty());
    }

    @Test(timeout = 5000)
    public void rejectedMappingIsNeverAdvertisedOrRemovedAndDisabledPortIsClosed() throws Exception {
        CountDownLatch disabledDone = new CountDownLatch(1);
        Gateway gateway = new Gateway() {
            @Override boolean open(int port) { super.open(port); return false; }
            @Override boolean close(int port) {
                super.close(port);
                disabledDone.countDown();
                return true;
            }
        };
        UpnpPortMapper mapper = new UpnpPortMapper(gateway);
        List<String> addresses = new ArrayList<>();
        mapper.configure(P2P, true, addresses::add);
        mapper.configure(QDN, false, addresses::add);
        assertTrue(disabledDone.await(2, TimeUnit.SECONDS));
        mapper.shutdown();
        assertEquals(Collections.singletonList(P2P), gateway.opened);
        assertEquals(Collections.singletonList(QDN), gateway.closed);
        assertTrue(addresses.isEmpty());
    }
}
