package org.qortal.controller.arbitrary;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.qortal.arbitrary.misc.Service;
import org.qortal.data.arbitrary.ArbitraryResourceStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ArbitraryResourceStateRegistryTests {

    private final ArbitraryResourceStateRegistry registry = ArbitraryResourceStateRegistry.getInstance();

    @Before
    public void beforeTest() {
        this.registry.clearForTesting();
    }

    @After
    public void afterTest() {
        this.registry.clearForTesting();
    }

    @Test
    public void concurrentRequestsShareOneReconciliation() throws Exception {
        AtomicInteger calculations = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        List<Future<ArbitraryResourceStatus>> futures = new ArrayList<>();
        for (int i = 0; i < 8; ++i) {
            futures.add(executor.submit(() -> {
                start.await();
                return this.registry.getOrReconcile(Service.WEBSITE, "single-flight", null, null, () -> {
                    calculations.incrementAndGet();
                    Thread.sleep(50L);
                    return new ArbitraryResourceStatus(ArbitraryResourceStatus.Status.DOWNLOADED, 4, 4);
                });
            }));
        }

        start.countDown();
        for (Future<ArbitraryResourceStatus> future : futures)
            assertEquals(ArbitraryResourceStatus.Status.DOWNLOADED,
                    future.get(5, TimeUnit.SECONDS).getStatus());
        executor.shutdownNow();
        assertEquals(1, calculations.get());
    }

    @Test
    public void resourceAndSignatureEventsInvalidateSnapshots() throws Exception {
        AtomicInteger calculations = new AtomicInteger();
        byte[] signature = new byte[64];
        signature[0] = 7;

        ArbitraryResourceStateRegistry.Reconciliation reconciliation = () -> {
            calculations.incrementAndGet();
            return new ArbitraryResourceStatus(ArbitraryResourceStatus.Status.DOWNLOADING, 1, 4);
        };

        this.registry.getOrReconcile(Service.WEBSITE, "events", null, signature, reconciliation);
        this.registry.getOrReconcile(Service.WEBSITE, "events", null, signature, reconciliation);
        assertEquals(1, calculations.get());

        this.registry.invalidateSignature(signature);
        this.registry.getOrReconcile(Service.WEBSITE, "events", null, signature, reconciliation);
        assertEquals(2, calculations.get());

        this.registry.invalidate(Service.WEBSITE, "events", null);
        this.registry.getOrReconcile(Service.WEBSITE, "events", null, signature, reconciliation);
        assertEquals(3, calculations.get());
    }

    @Test
    public void mutationDuringCalculationLeavesSnapshotDirty() throws Exception {
        AtomicInteger calculations = new AtomicInteger();
        CountDownLatch calculating = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();

        Future<ArbitraryResourceStatus> first = executor.submit(() -> this.registry.getOrReconcile(
                Service.WEBSITE, "racing-event", null, null, () -> {
                    calculations.incrementAndGet();
                    calculating.countDown();
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                    return new ArbitraryResourceStatus(ArbitraryResourceStatus.Status.DOWNLOADING);
                }));

        assertTrue(calculating.await(5, TimeUnit.SECONDS));
        this.registry.invalidate(Service.WEBSITE, "racing-event", null);
        release.countDown();
        first.get(5, TimeUnit.SECONDS);

        this.registry.getOrReconcile(Service.WEBSITE, "racing-event", null, null, () -> {
            calculations.incrementAndGet();
            return new ArbitraryResourceStatus(ArbitraryResourceStatus.Status.DOWNLOADED);
        });
        executor.shutdownNow();
        assertEquals(2, calculations.get());
    }
}
