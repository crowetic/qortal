package org.qortal.controller.arbitrary;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.qortal.arbitrary.ArbitraryDataBuildQueueItem;
import org.qortal.arbitrary.ArbitraryDataFile.ResourceIdType;
import org.qortal.arbitrary.misc.Service;
import org.qortal.controller.Controller;
import org.qortal.data.arbitrary.ArbitraryResourceStatus;
import org.qortal.utils.NTP;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.*;
import static org.qortal.data.arbitrary.ArbitraryResourceStatus.Status.*;

public class ArbitraryDataBuilderThreadTests {

    private final ArbitraryDataBuildManager manager = ArbitraryDataBuildManager.getInstance();
    private final ArbitraryResourceStateRegistry registry = ArbitraryResourceStateRegistry.getInstance();
    private Map<String, ArbitraryDataBuildQueueItem> originalQueue;
    private Map<String, ArbitraryDataBuildQueueItem> originalFailures;
    private Object originalStopping;
    private Object originalOffset;
    private Object originalOffsetSet;
    private Thread worker;
    private ExecutorService requests;
    private final CountDownLatch releaseStatus = new CountDownLatch(1);

    @Before
    public void setUp() throws Exception {
        this.originalStopping = field(Controller.class, "isStopping").get(null);
        this.originalOffset = field(NTP.class, "offset").get(null);
        this.originalOffsetSet = field(NTP.class, "isOffsetSet").get(null);
        field(Controller.class, "isStopping").set(null, false);
        NTP.setFixedOffset(0L);
        this.originalQueue = this.manager.arbitraryDataBuildQueue;
        this.originalFailures = this.manager.arbitraryDataFailedBuilds;
        this.manager.arbitraryDataBuildQueue = Collections.synchronizedMap(new LinkedHashMap<>());
        this.manager.arbitraryDataFailedBuilds = Collections.synchronizedMap(new HashMap<>());
        this.registry.clearForTesting();
        this.requests = Executors.newFixedThreadPool(2, task -> {
            Thread thread = new Thread(task, "QDN test request");
            thread.setDaemon(true);
            return thread;
        });
    }

    @After
    public void tearDown() throws Exception {
        // Release reconciliation before stopping the worker, including on the old deadlocking code.
        this.releaseStatus.countDown();
        field(Controller.class, "isStopping").set(null, true);
        if (this.worker != null) {
            this.worker.interrupt();
            this.worker.join(5_000L);
        }
        this.requests.shutdownNow();
        assertTrue(this.requests.awaitTermination(5, TimeUnit.SECONDS));
        assertTrue(this.worker == null || !this.worker.isAlive());
        this.manager.arbitraryDataBuildQueue = this.originalQueue;
        this.manager.arbitraryDataFailedBuilds = this.originalFailures;
        this.registry.clearForTesting();
        field(NTP.class, "offset").set(null, this.originalOffset);
        field(NTP.class, "isOffsetSet").set(null, this.originalOffsetSet);
        field(Controller.class, "isStopping").set(null, this.originalStopping);
    }

    @Test
    public void statusWaitDoesNotBlockQueueReaders() throws Exception {
        TestQueueItem item = new TestQueueItem("status-wait");
        Future<ArbitraryResourceStatus> status = startPendingStatus(item);
        startWorker(item);
        awaitStatusWait();

        // This is the reverse edge in the production deadlock: another status request needs
        // the queue while the builder is joining an in-flight resource reconciliation.
        Future<Boolean> queueRead = this.requests.submit(() -> this.manager.isInBuildQueue(item));
        assertTrue("Queue reads must finish while reconciliation is pending", queueRead.get(2, TimeUnit.SECONDS));
        assertFalse("The item must already be claimed", item.isQueued());
        this.releaseStatus.countDown();
        assertEquals(DOWNLOADED, status.get(5, TimeUnit.SECONDS).getStatus());
        await(() -> item.builds.get() == 1 && !this.manager.isInBuildQueue(item));
    }

    @Test
    public void duplicateEnqueuePreservesClaimedItem() {
        TestQueueItem original = new TestQueueItem("duplicate");
        assertTrue(this.manager.addToBuildQueue(original));
        original.prepareForBuild();
        assertTrue(this.manager.addToBuildQueue(new TestQueueItem("duplicate")));
        assertSame(original, this.manager.arbitraryDataBuildQueue.get(original.getUniqueKey()));
        assertFalse(original.isQueued());
    }

    @Test
    public void finishingOldBuildDoesNotRemoveReplacement() throws Exception {
        TestQueueItem original = new TestQueueItem("replacement");
        startPendingStatus(original);
        startWorker(original);
        awaitStatusWait();

        // Even an explicit replacement must not be removed by the previous owner.
        TestQueueItem replacement = new TestQueueItem("replacement");
        replacement.prepareForBuild();
        this.manager.arbitraryDataBuildQueue.put(original.getUniqueKey(), replacement);
        this.releaseStatus.countDown();
        await(() -> original.builds.get() == 1 && this.worker.getState() == Thread.State.WAITING);
        assertSame(replacement, this.manager.arbitraryDataBuildQueue.get(original.getUniqueKey()));
    }

    @Test
    public void nullStatusFailsItemAndWorkerProcessesNextResource() throws Exception {
        TestQueueItem failed = new TestQueueItem("null-status");
        cacheStatus(failed, null);
        startWorker(failed);
        await(() -> this.manager.isInFailedBuildsList(failed) && !this.manager.isInBuildQueue(failed));
        assertEquals(0, failed.builds.get());

        TestQueueItem next = new TestQueueItem("after-null-status");
        cacheStatus(next, new ArbitraryResourceStatus(DOWNLOADED));
        await(() -> next.builds.get() == 1 && !this.manager.isInBuildQueue(next));
        assertTrue(this.worker.isAlive());
    }

    @Test
    public void unpublishedResourceIsRemovedWithoutBuilding() throws Exception {
        TestQueueItem item = new TestQueueItem("unpublished");
        cacheStatus(item, new ArbitraryResourceStatus(NOT_PUBLISHED));
        startWorker(item);
        await(() -> !this.manager.isInBuildQueue(item));
        assertEquals(0, item.builds.get());
        assertFalse(this.manager.isInFailedBuildsList(item));
    }

    @Test
    public void agingPromotesOlderWorkAboveNewerRequests() throws Exception {
        NTP.setFixedOffset(-120_000L);
        TestQueueItem older = new TestQueueItem("older");
        NTP.setFixedOffset(0L);
        TestQueueItem newer = new TestQueueItem("newer");
        newer.setPriority(10);
        assertTrue(this.manager.addToBuildQueue(newer));
        assertTrue(this.manager.addToBuildQueue(older));
        assertSame(older, this.manager.takeNextBuild(false));
        assertSame(newer, this.manager.takeNextBuild(false));
    }

    @Test
    public void fileSizeDrivesSchedulingWithoutOverwritingCallerPriority() throws Exception {
        TestQueueItem small = new TestQueueItem("small");
        small.size = 1024L;
        small.setPriority(1);
        TestQueueItem large = new TestQueueItem("large");
        large.size = 101L * 1024 * 1024;
        large.setPriority(1);
        assertTrue(this.manager.addToBuildQueue(large));
        assertTrue(this.manager.addToBuildQueue(small));
        assertEquals(1, small.getPriority());
        assertEquals(1, large.getPriority());
        assertTrue(large.isLargeFile());
        assertFalse(small.isLargeFile());
        assertSame(small, this.manager.takeNextBuild(false));
        assertSame(large, this.manager.takeNextBuild(true));
    }

    @Test
    public void idleWorkerSleepsUntilNewWorkArrives() throws Exception {
        this.worker = new Thread(new ArbitraryDataBuilderThread());
        this.worker.setDaemon(true);
        this.worker.start();
        await(() -> this.worker.getState() == Thread.State.WAITING);
        TestQueueItem item = new TestQueueItem("wake-worker");
        cacheStatus(item, new ArbitraryResourceStatus(DOWNLOADED));
        await(() -> item.builds.get() == 1 && !this.manager.isInBuildQueue(item));
        await(() -> this.worker.getState() == Thread.State.WAITING);
    }

    @Test
    public void largeFileWorkerWakesForLargeWorkAndLeavesSmallWorkQueued() throws Exception {
        TestQueueItem small = new TestQueueItem("small-waiting");
        small.size = 1024L;
        assertTrue(this.manager.addToBuildQueue(small));
        Future<ArbitraryDataBuildQueueItem> selection = this.requests.submit(() -> this.manager.takeNextBuild(true));
        TestQueueItem large = new TestQueueItem("large-wakeup");
        large.size = 101L * 1024 * 1024;
        assertTrue(this.manager.addToBuildQueue(large));
        assertSame(large, selection.get(5, TimeUnit.SECONDS));
        assertTrue(small.isQueued());
    }

    @Test
    public void cleanupRetainsWaitingAndRunningBuildsRegardlessOfAge() throws Exception {
        TestQueueItem running = new TestQueueItem("long-build");
        TestQueueItem waiting = new TestQueueItem("long-wait");
        assertTrue(this.manager.addToBuildQueue(running));
        assertSame(running, this.manager.takeNextBuild(false));
        assertTrue(this.manager.addToBuildQueue(waiting));
        this.manager.cleanupQueues(NTP.getTime() + TimeUnit.HOURS.toMillis(2));
        assertSame(running, this.manager.arbitraryDataBuildQueue.get(running.getUniqueKey()));
        assertTrue(this.manager.isInBuildQueue(waiting));
        assertTrue(this.manager.addToBuildQueue(new TestQueueItem("long-build")));
        assertSame(running, this.manager.arbitraryDataBuildQueue.get(running.getUniqueKey()));
    }

    @Test
    public void failureCooldownStartsWhenWorkFails() {
        NTP.setFixedOffset(-TimeUnit.HOURS.toMillis(2));
        TestQueueItem item = new TestQueueItem("long-failure");
        item.prepareForBuild();
        NTP.setFixedOffset(0L);
        item.setFailed(true);
        this.manager.addToFailedBuildsList(item);
        long now = NTP.getTime();
        this.manager.cleanupQueues(now + 1000L);
        assertTrue(this.manager.isInFailedBuildsList(item));
        this.manager.cleanupQueues(now + ArbitraryDataBuildQueueItem.FAILURE_TIMEOUT + 1000L);
        assertFalse(this.manager.isInFailedBuildsList(item));
    }

    @Test
    public void metadataLookupNeverHoldsTheQueueMonitor() {
        TestQueueItem item = new TestQueueItem("metadata-lock") {
            @Override
            protected Long fetchDataSize() {
                assertFalse(Thread.holdsLock(manager.arbitraryDataBuildQueue));
                return 1024L;
            }
        };
        assertTrue(this.manager.addToBuildQueue(item));
    }

    @Test
    public void shutdownReleasesWorkersWaitingForWork() throws Exception {
        ArbitraryDataBuildManager localManager = new ArbitraryDataBuildManager();
        Future<ArbitraryDataBuildQueueItem> selection = this.requests.submit(() -> localManager.takeNextBuild(false));
        localManager.shutdown();
        assertNull(selection.get(5, TimeUnit.SECONDS));
    }

    private Future<ArbitraryResourceStatus> startPendingStatus(TestQueueItem item) throws Exception {
        CountDownLatch calculating = new CountDownLatch(1);
        Future<ArbitraryResourceStatus> result = this.requests.submit(() -> this.registry.getOrReconcile(
                item.getService(), item.getResourceId(), item.getIdentifier(), null, () -> {
                    calculating.countDown();
                    assertTrue(this.releaseStatus.await(10, TimeUnit.SECONDS));
                    return new ArbitraryResourceStatus(DOWNLOADED);
                }));
        assertTrue(calculating.await(5, TimeUnit.SECONDS));
        return result;
    }

    private void cacheStatus(TestQueueItem item, ArbitraryResourceStatus status) throws Exception {
        synchronized (this.manager.arbitraryDataBuildQueue) {
            assertTrue(this.manager.addToBuildQueue(item));
            this.registry.getOrReconcile(item.getService(), item.getResourceId(), item.getIdentifier(),
                    null, () -> status);
        }
    }

    private void startWorker(TestQueueItem item) {
        assertTrue(this.manager.addToBuildQueue(item));
        this.worker = new Thread(new ArbitraryDataBuilderThread());
        this.worker.setDaemon(true);
        this.worker.start();
    }

    private void awaitStatusWait() throws Exception {
        await(() -> {
            if (this.worker.getState() != Thread.State.WAITING)
                return false;
            for (StackTraceElement frame : this.worker.getStackTrace()) {
                if (frame.getClassName().equals(ArbitraryResourceStateRegistry.class.getName())
                        && frame.getMethodName().equals("getOrReconcile"))
                    return true;
            }
            return false;
        });
    }

    private static void await(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            assertTrue("Timed out waiting for worker", System.nanoTime() < deadline);
            Thread.sleep(10L);
        }
    }

    private static Field field(Class<?> type, String name) throws Exception {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }

    private static class TestQueueItem extends ArbitraryDataBuildQueueItem {
        private final AtomicInteger builds = new AtomicInteger();
        private Long size;

        @Override
        protected Long fetchDataSize() {
            return this.size;
        }

        private TestQueueItem(String name) {
            super(name, ResourceIdType.NAME, Service.WEBSITE, null);
        }

        @Override
        public void build() {
            this.builds.incrementAndGet();
        }
    }
}
