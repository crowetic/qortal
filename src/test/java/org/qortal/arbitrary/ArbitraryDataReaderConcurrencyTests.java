package org.qortal.arbitrary;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.qortal.arbitrary.ArbitraryDataFile.ResourceIdType;
import org.qortal.arbitrary.misc.Service;
import org.qortal.controller.arbitrary.ArbitraryDataBuildManager;
import org.qortal.controller.arbitrary.ArbitraryResourceStateRegistry;
import org.qortal.data.arbitrary.ArbitraryResourceStatus;
import org.qortal.repository.DataException;
import org.qortal.test.common.Common;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class ArbitraryDataReaderConcurrencyTests extends Common {
    private String resource;
    private ExecutorService executor;
    private final CountDownLatch fetching = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);
    private final AtomicInteger fetches = new AtomicInteger();
    private TestReader owner;

    @Before
    public void setUp() throws Exception {
        Common.useDefaultSettings();
        this.resource = UUID.randomUUID().toString();
        this.executor = Executors.newFixedThreadPool(2);
        this.owner = new TestReader(false, 60_000L);
    }

    @After
    public void tearDown() throws Exception {
        this.release.countDown();
        this.executor.shutdownNow();
        assertTrue(this.executor.awaitTermination(5, TimeUnit.SECONDS));
        Path working = this.owner.getUncompressedPath().getParent();
        if (Files.isDirectory(working))
            FileUtils.deleteDirectory(working.toFile());
        else
            Files.deleteIfExists(working);
        assertFalse(ArbitraryDataBuildManager.getInstance().getBuildInProgress());
    }

    @Test
    public void concurrentReadersShareCompletedBuildWithoutServingPartialFiles() throws Exception {
        Future<?> first = startOwner();
        TestReader follower = new TestReader(false, 60_000L);
        AtomicReference<Thread> waitingThread = new AtomicReference<>();
        Future<?> second = this.executor.submit(() -> {
            waitingThread.set(Thread.currentThread());
            follower.loadSynchronously(true);
            return null;
        });
        awaitFollower(waitingThread);
        assertFalse(follower.isCachedDataAvailable());
        assertFalse(second.isDone());
        assertTrue(follower.loadAsynchronously(false, 1));
        this.release.countDown();
        first.get(5, TimeUnit.SECONDS);
        second.get(5, TimeUnit.SECONDS);
        assertEquals(1, this.fetches.get());
        assertEquals(this.owner.getFilePath(), follower.getFilePath());
        assertEquals("complete", Files.readString(follower.getFilePath().resolve("payload")));
    }

    @Test
    public void waitingCallerTimeoutCannotDeleteOrReleaseOwnersOutput() throws Exception {
        Future<?> first = startOwner();
        TestReader follower = new TestReader(false, 50L);
        try {
            follower.loadSynchronously(true);
            fail("Expected a timeout");
        } catch (DataException e) {
            assertTrue(e.getCause() instanceof TimeoutException);
        }
        assertTrue(this.owner.isBuilding());
        assertEquals("partial", Files.readString(this.owner.getUncompressedPath().resolve("payload")));
        this.release.countDown();
        first.get(5, TimeUnit.SECONDS);
        assertEquals(1, this.fetches.get());
        assertEquals("complete", Files.readString(this.owner.getFilePath().resolve("payload")));
    }

    @Test
    public void interruptingWaiterLeavesOwnerRunning() throws Exception {
        assertInterruptingWaiterLeavesOwnerRunning(60_000L);
    }

    @Test
    public void interruptingBuildWorkerWaitLeavesOwnerRunning() throws Exception {
        assertInterruptingWaiterLeavesOwnerRunning(0L);
    }

    private void assertInterruptingWaiterLeavesOwnerRunning(long waitTimeoutMillis) throws Exception {
        Future<?> first = startOwner();
        TestReader follower = new TestReader(false, 60_000L);
        AtomicReference<Thread> waitingThread = new AtomicReference<>();
        Future<Boolean> second = this.executor.submit(() -> {
            waitingThread.set(Thread.currentThread());
            try {
                follower.loadSynchronously(true, waitTimeoutMillis);
                return false;
            } catch (DataException e) {
                return Thread.currentThread().isInterrupted() && e.getCause() instanceof InterruptedException;
            }
        });
        awaitFollower(waitingThread);
        waitingThread.get().interrupt();
        assertTrue(second.get(5, TimeUnit.SECONDS));
        assertTrue(this.owner.isBuilding());
        assertTrue(Files.exists(this.owner.getUncompressedPath().resolve("payload")));
        this.release.countDown();
        first.get(5, TimeUnit.SECONDS);
        assertEquals(1, this.fetches.get());
    }

    @Test
    public void failureIsSharedAndNextCallerCanRetry() throws Exception {
        this.owner = new TestReader(true, 60_000L);
        Future<?> first = startOwner();
        TestReader follower = new TestReader(false, 60_000L);
        AtomicReference<Thread> waitingThread = new AtomicReference<>();
        Future<?> second = this.executor.submit(() -> {
            waitingThread.set(Thread.currentThread());
            follower.loadSynchronously(true);
            return null;
        });
        awaitFollower(waitingThread);
        this.release.countDown();
        for (Future<?> future : new Future<?>[] { first, second }) {
            try {
                future.get(5, TimeUnit.SECONDS);
                fail("Expected the shared build failure");
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof IOException);
            }
        }
        assertEquals(1, this.fetches.get());
        assertFalse(this.owner.isBuilding());
        assertFalse(Files.exists(this.owner.getUncompressedPath()));
        TestReader retry = new TestReader(false, 60_000L);
        retry.loadSynchronously(true);
        assertEquals(2, this.fetches.get());
        assertEquals("complete", Files.readString(retry.getFilePath().resolve("payload")));
    }

    @Test
    public void preparationFailureDoesNotLeakOwnershipOrBuildCounter() throws Exception {
        Path working = this.owner.getUncompressedPath().getParent();
        Files.createDirectories(working.getParent());
        Files.writeString(working, "a file prevents creating the working directory");
        try {
            this.owner.loadSynchronously(true);
            fail("Expected working directory creation to fail");
        } catch (DataException expected) {
            assertFalse(this.owner.isBuilding());
            assertFalse(ArbitraryDataBuildManager.getInstance().getBuildInProgress());
        }
        Files.delete(working);
        this.release.countDown();
        this.owner.loadSynchronously(true);
        assertEquals(1, this.fetches.get());
    }

    @Test
    public void forcedRebuildDoesNotAcceptConcurrentCacheHit() throws Exception {
        Files.createDirectories(this.owner.getUncompressedPath());
        Files.writeString(this.owner.getUncompressedPath().resolve("payload"), "old cache");
        CountDownLatch cached = new CountDownLatch(1);
        CountDownLatch resumeCache = new CountDownLatch(1);
        TestReader cacheReader = new TestReader(false, 60_000L) {
            @Override
            boolean loadFromCache(boolean overwrite) {
                boolean available = super.loadFromCache(overwrite);
                cached.countDown();
                try {
                    if (!resumeCache.await(5, TimeUnit.SECONDS))
                        throw new AssertionError("Cache reader was not released");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                return available;
            }
        };
        Future<?> first = this.executor.submit(() -> {
            cacheReader.loadSynchronously(false);
            return null;
        });
        try {
            assertTrue(cached.await(5, TimeUnit.SECONDS));
            TestReader rebuilding = new TestReader(false, 60_000L);
            AtomicReference<Thread> waitingThread = new AtomicReference<>();
            Future<?> second = this.executor.submit(() -> {
                waitingThread.set(Thread.currentThread());
                rebuilding.loadSynchronously(true);
                return null;
            });
            awaitFollower(waitingThread);
            this.release.countDown();
            resumeCache.countDown();
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
            assertEquals(1, this.fetches.get());
            assertEquals("complete", Files.readString(rebuilding.getFilePath().resolve("payload")));
        } finally {
            resumeCache.countDown();
        }
    }

    @Test
    public void buildWorkerCanJoinBeyondNormalCallerTimeout() throws Exception {
        Future<?> first = startOwner();
        TestReader follower = new TestReader(false, 50L);
        AtomicReference<Thread> waitingThread = new AtomicReference<>();
        Future<?> second = this.executor.submit(() -> {
            waitingThread.set(Thread.currentThread());
            follower.loadSynchronously(true, 0L);
            return null;
        });
        awaitFollower(waitingThread);
        try {
            second.get(150, TimeUnit.MILLISECONDS);
            fail("Queue worker must keep waiting for the active build");
        } catch (TimeoutException expected) {
            assertTrue(this.owner.isBuilding());
        }
        this.release.countDown();
        first.get(5, TimeUnit.SECONDS);
        second.get(5, TimeUnit.SECONDS);
        assertEquals(1, this.fetches.get());
        assertEquals("complete", Files.readString(follower.getFilePath().resolve("payload")));
    }

    @Test
    public void fatalBuildFailureDoesNotLeavePartialDataAsUsableCache() throws Exception {
        this.release.countDown();
        this.owner = new TestReader(false, 60_000L) {
            @Override
            void fetch() throws IOException, DataException {
                super.fetch();
                throw new AssertionError("Deliberate fatal build failure");
            }
        };
        try {
            this.owner.loadSynchronously(true);
            fail("Expected fatal build failure");
        } catch (AssertionError expected) {
            assertEquals("Deliberate fatal build failure", expected.getMessage());
        }
        assertFalse(this.owner.isBuilding());
        assertFalse(this.owner.isCachedDataAvailable());
        assertFalse(Files.exists(this.owner.getUncompressedPath()));
    }

    @Test
    public void synchronousBuildInvalidatesStatusOnStartAndCompletion() throws Exception {
        this.owner = new TestReader(false, 60_000L, ResourceIdType.NAME);
        ArbitraryResourceStateRegistry registry = ArbitraryResourceStateRegistry.getInstance();
        AtomicInteger calculations = new AtomicInteger();
        ArbitraryResourceStateRegistry.Reconciliation calculate = () -> {
            calculations.incrementAndGet();
            return new ArbitraryResourceStatus(this.owner.isBuilding()
                    ? ArbitraryResourceStatus.Status.BUILDING : ArbitraryResourceStatus.Status.READY);
        };
        assertEquals(ArbitraryResourceStatus.Status.READY,
                registry.getOrReconcile(Service.ARBITRARY_DATA, this.resource, null, null, calculate).getStatus());
        Future<?> first = startOwner();
        assertEquals(ArbitraryResourceStatus.Status.BUILDING,
                registry.getOrReconcile(Service.ARBITRARY_DATA, this.resource, null, null, calculate).getStatus());
        this.release.countDown();
        first.get(5, TimeUnit.SECONDS);
        assertEquals(ArbitraryResourceStatus.Status.READY,
                registry.getOrReconcile(Service.ARBITRARY_DATA, this.resource, null, null, calculate).getStatus());
        assertEquals(3, calculations.get());
    }

    @Test
    public void apiWaitJoinsOwnerWithoutStartingAnotherBuild() throws Exception {
        Future<?> first = startOwner();
        TestReader follower = new TestReader(false, 50L);
        AtomicReference<Thread> waiting = new AtomicReference<>();
        Future<?> second = this.executor.submit(() -> {
            waiting.set(Thread.currentThread());
            follower.loadSynchronouslyWhenReady(true, () -> false);
            return null;
        });
        awaitFollower(waiting);
        try {
            second.get(100, TimeUnit.MILLISECONDS);
            fail("API wait must not introduce the reader's shorter timeout");
        } catch (TimeoutException expected) { /* Owner is deliberately still building. */ }
        this.release.countDown();
        first.get(5, TimeUnit.SECONDS);
        second.get(5, TimeUnit.SECONDS);
        assertEquals(1, this.fetches.get());
        assertEquals("complete", Files.readString(follower.getFilePath().resolve("payload")));
    }

    @Test
    public void forcedApiRebuildStillRespectsQueueAfterJoiningCacheCheck() throws Exception {
        CountDownLatch cacheCheck = new CountDownLatch(1), releaseCache = new CountDownLatch(1);
        CountDownLatch queueChecked = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicBoolean queued = new java.util.concurrent.atomic.AtomicBoolean(true);
        TestReader cacheReader = new TestReader(false, 60_000L) {
            @Override boolean loadFromCache(boolean overwrite) {
                cacheCheck.countDown();
                try {
                    if (!releaseCache.await(5, TimeUnit.SECONDS)) throw new AssertionError("Cache check not released");
                } catch (InterruptedException e) { throw new AssertionError(e); }
                return true;
            }
        };
        Future<?> checking = this.executor.submit(() -> { cacheReader.loadSynchronously(false); return null; });
        assertTrue(cacheCheck.await(5, TimeUnit.SECONDS));
        TestReader follower = new TestReader(false, 60_000L) {
            @Override boolean isQueuedForBuild() { queueChecked.countDown(); return queued.get(); }
        };
        AtomicReference<Thread> waiting = new AtomicReference<>();
        Future<?> building = this.executor.submit(() -> {
            waiting.set(Thread.currentThread());
            follower.loadSynchronouslyWhenReady(true, () -> false);
            return null;
        });
        try {
            awaitFollower(waiting);
            releaseCache.countDown();
            checking.get(5, TimeUnit.SECONDS);
            assertTrue(queueChecked.await(5, TimeUnit.SECONDS));
            assertFalse(building.isDone());
            assertEquals(0, this.fetches.get());
            queued.set(false);
            this.release.countDown();
            building.get(5, TimeUnit.SECONDS);
            assertEquals(1, this.fetches.get());
        } finally {
            releaseCache.countDown();
            queued.set(false);
            this.release.countDown();
        }
    }

    @Test
    public void manyApiWaitersShareOneBuildAndReceiveCompleteOutput() throws Exception {
        Future<?> first = startOwner();
        ExecutorService waiters = Executors.newFixedThreadPool(150);
        java.util.List<Future<?>> calls = new java.util.ArrayList<>();
        java.util.List<AtomicReference<Thread>> threads = new java.util.ArrayList<>();
        try {
            for (int i = 0; i < 150; ++i) {
                AtomicReference<Thread> thread = new AtomicReference<>();
                threads.add(thread);
                calls.add(waiters.submit(() -> {
                    TestReader follower = new TestReader(false, 60_000L);
                    thread.set(Thread.currentThread());
                    follower.loadSynchronouslyWhenReady(true, () -> false);
                    assertEquals("complete", Files.readString(follower.getFilePath().resolve("payload")));
                    return null;
                }));
            }
            for (AtomicReference<Thread> thread : threads) awaitFollower(thread);
            this.release.countDown();
            first.get(5, TimeUnit.SECONDS);
            for (Future<?> call : calls) call.get(10, TimeUnit.SECONDS);
            assertEquals(1, this.fetches.get());
        } finally {
            this.release.countDown();
            waiters.shutdownNow();
            assertTrue(waiters.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void apiShutdownStopsWaitingWithoutCancellingOwner() throws Exception {
        Future<?> first = startOwner();
        TestReader follower = new TestReader(false, 60_000L);
        java.util.concurrent.atomic.AtomicBoolean stopping = new java.util.concurrent.atomic.AtomicBoolean();
        AtomicReference<Thread> waiting = new AtomicReference<>();
        Future<?> second = this.executor.submit(() -> {
            waiting.set(Thread.currentThread());
            follower.loadSynchronouslyWhenReady(true, stopping::get);
            return null;
        });
        awaitFollower(waiting);
        stopping.set(true);
        second.get(5, TimeUnit.SECONDS);
        assertFalse(first.isDone());
        assertTrue(this.owner.isBuilding());
        assertEquals("partial", Files.readString(this.owner.getUncompressedPath().resolve("payload")));
        this.release.countDown();
        first.get(5, TimeUnit.SECONDS);
        assertEquals(1, this.fetches.get());
    }

    @Test
    public void shutdownWhileQueuedDoesNotStartABuild() throws Exception {
        CountDownLatch checked = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicBoolean stopping = new java.util.concurrent.atomic.AtomicBoolean();
        TestReader queued = new TestReader(false, 60_000L) {
            @Override boolean isQueuedForBuild() { checked.countDown(); return true; }
        };
        Future<?> waiting = this.executor.submit(() -> {
            queued.loadSynchronouslyWhenReady(false, stopping::get);
            return null;
        });
        try {
            assertTrue(checked.await(5, TimeUnit.SECONDS));
            stopping.set(true);
            waiting.get(5, TimeUnit.SECONDS);
            assertEquals(0, this.fetches.get());
            assertFalse(Files.exists(queued.getUncompressedPath()));
        } finally {
            stopping.set(true);
        }
    }

    @Test
    public void queuedApiWaitIsInterruptibleWithoutBypassingScheduler() throws Exception {
        CountDownLatch checked = new CountDownLatch(1), finished = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();
        java.util.concurrent.atomic.AtomicBoolean interrupted = new java.util.concurrent.atomic.AtomicBoolean();
        TestReader queued = new TestReader(false, 50L) {
            @Override boolean isQueuedForBuild() { checked.countDown(); return true; }
        };
        Future<?> waiting = this.executor.submit(() -> {
            try {
                queued.loadSynchronouslyWhenReady(false, () -> false);
            } catch (DataException expected) {
                error.set(expected.getCause());
                interrupted.set(Thread.currentThread().isInterrupted());
            } catch (Exception unexpected) { error.set(unexpected); }
            finally { finished.countDown(); }
        });
        assertTrue(checked.await(5, TimeUnit.SECONDS));
        waiting.cancel(true);
        assertTrue(finished.await(5, TimeUnit.SECONDS));
        assertTrue(error.get() instanceof InterruptedException);
        assertTrue(interrupted.get());
        assertEquals(0, this.fetches.get());
        assertFalse(Files.exists(queued.getUncompressedPath()));
    }

    private Future<?> startOwner() throws Exception {
        Future<?> future = this.executor.submit(() -> {
            this.owner.loadSynchronously(true);
            return null;
        });
        assertTrue(this.fetching.await(5, TimeUnit.SECONDS));
        return future;
    }

    private static void awaitFollower(AtomicReference<Thread> reference) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            Thread thread = reference.get();
            if (thread != null && (thread.getState() == Thread.State.TIMED_WAITING
                    || thread.getState() == Thread.State.WAITING)) {
                for (StackTraceElement frame : thread.getStackTrace()) {
                    if (frame.getMethodName().equals("awaitLoad"))
                        return;
                }
            }
            Thread.sleep(10L);
        }
        fail("Follower did not wait for the active build");
    }

    @Test
    public void boundedQueuedWaitDoesNotStartOrRemoveWork() throws Exception {
        TestReader queued = new TestReader(false, 60_000L) {
            @Override boolean isQueuedForBuild() { return true; }
        };
        long started = System.nanoTime();
        try {
            queued.loadSynchronouslyWhenReady(false, () -> false, 50L);
            fail("A queue wait must expire");
        } catch (DataException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("Timed out"));
        }
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) < 5000L);
        assertEquals(0, this.fetches.get());
        assertTrue(queued.isQueuedForBuild());
        assertFalse(Files.exists(queued.getUncompressedPath()));
    }

    @Test
    public void boundedConcurrentWaitLeavesActiveBuildIntact() throws Exception {
        Future<?> first = startOwner();
        TestReader follower = new TestReader(false, 60_000L);
        try {
            follower.loadSynchronouslyWhenReady(false, () -> false, 50L);
            fail("A concurrent request wait must expire");
        } catch (DataException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().toLowerCase().contains("tim"));
        }
        assertTrue(this.owner.isBuilding());
        assertEquals("partial", Files.readString(this.owner.getUncompressedPath().resolve("payload")));
        this.release.countDown();
        first.get(5, TimeUnit.SECONDS);
        assertEquals("complete", Files.readString(this.owner.getFilePath().resolve("payload")));
    }

    private class TestReader extends ArbitraryDataReader {
        private final boolean failFetch;
        private final long timeout;

        TestReader(boolean failFetch, long timeout) throws DataException {
            this(failFetch, timeout, ResourceIdType.FILE_HASH);
        }

        TestReader(boolean failFetch, long timeout, ResourceIdType type) throws DataException {
            super(resource, type, Service.ARBITRARY_DATA, null);
            this.failFetch = failFetch;
            this.timeout = timeout;
        }

        @Override
        long loadWaitTimeoutMillis() {
            return this.timeout;
        }

        @Override
        void fetch() throws IOException, DataException {
            fetches.incrementAndGet();
            Files.createDirectories(this.getUncompressedPath());
            Files.writeString(this.getUncompressedPath().resolve("payload"), "partial");
            fetching.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS))
                    throw new DataException("Test build was not released");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DataException("Test build interrupted", e);
            }
            if (this.failFetch)
                throw new IOException("Deliberate fetch failure");
            Files.writeString(this.getUncompressedPath().resolve("payload"), "complete");
            this.setFilePath(this.getUncompressedPath());
        }
    }
}
