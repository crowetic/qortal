package org.qortal.controller.arbitrary;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.qortal.arbitrary.ArbitraryDataBuildQueueItem;
import org.qortal.arbitrary.ArbitraryDataFile.ResourceIdType;
import org.qortal.arbitrary.misc.Service;
import org.qortal.controller.Controller;
import org.qortal.utils.NTP;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class ArbitraryDataBuildManager extends Thread {

    private static final Logger LOGGER = LogManager.getLogger(ArbitraryDataBuildManager.class);

    private static ArbitraryDataBuildManager instance;

    private volatile boolean isStopping = false;
    private final AtomicInteger buildsInProgress = new AtomicInteger();
    private volatile ExecutorService arbitraryDataBuildExecutor;

    /**
     * Map to keep track of arbitrary transaction resources currently being built (or queued).
     */
    public Map<String, ArbitraryDataBuildQueueItem> arbitraryDataBuildQueue = Collections.synchronizedMap(new LinkedHashMap<>());

    /**
     * Map to keep track of failed arbitrary transaction builds.
     */
    public Map<String, ArbitraryDataBuildQueueItem> arbitraryDataFailedBuilds = Collections.synchronizedMap(new HashMap<>());


    public ArbitraryDataBuildManager() {

    }

    @Override
    public void run() {
        Thread.currentThread().setName("Arbitrary Data Build Manager");

        try {
            // Use a fixed thread pool to execute the arbitrary data build actions
            // Combined approach: 1 thread reserved for large files + 4 threads with priority aging
            // This prevents small files from being blocked while ensuring large files don't starve
            int threadCount = 5;
            this.arbitraryDataBuildExecutor = Executors.newFixedThreadPool(threadCount);
            for (int i = 0; i < threadCount; i++) {
                // Thread 0 is reserved for large files, threads 1-4 use priority aging
                boolean isLargeFileThread = (i == 0);
                arbitraryDataBuildExecutor.execute(new ArbitraryDataBuilderThread(isLargeFileThread));
            }

            while (!isStopping) {
                // Nothing to do yet
                Thread.sleep(5000);
            }

        } catch (InterruptedException e) {
            // Fall-through to exit thread...
        } finally {
            if (this.arbitraryDataBuildExecutor != null)
                this.arbitraryDataBuildExecutor.shutdownNow();
        }
    }

    public static synchronized ArbitraryDataBuildManager getInstance() {
        if (instance == null)
            instance = new ArbitraryDataBuildManager();

        return instance;
    }

    public void shutdown() {
        isStopping = true;
        synchronized (this.arbitraryDataBuildQueue) {
            this.arbitraryDataBuildQueue.notifyAll();
        }
        if (this.arbitraryDataBuildExecutor != null)
            this.arbitraryDataBuildExecutor.shutdownNow();
        this.interrupt();
    }


    public void cleanupQueues(Long now) {
        if (now == null) {
            return;
        }
        // Waiting and running builds retain their ownership until the worker finishes. Removing
        // them by age neither cancels the work nor makes its partially written output safe.
        arbitraryDataFailedBuilds.entrySet().removeIf(entry -> entry.getValue().hasReachedFailureTimeout(now));
    }

    // Build queue

    public boolean addToBuildQueue(ArbitraryDataBuildQueueItem queueItem) {
        String key = queueItem.getUniqueKey();
        if (key == null) {
            return false;
        }

        if (this.arbitraryDataBuildQueue == null) {
            return false;
        }

        if (NTP.getTime() == null) {
            // Can't use queues until we have synced the time
            return false;
        }

        // Don't add builds that have failed recently
        if (this.isInFailedBuildsList(queueItem)) {
            return false;
        }

        if (this.arbitraryDataBuildQueue.containsKey(key))
            return true;

        // Repository access must never run under the queue monitor.
        queueItem.initializeSchedulingMetadata();
        synchronized (this.arbitraryDataBuildQueue) {
            if (this.isStopping)
                return false;
            if (this.arbitraryDataBuildQueue.putIfAbsent(key, queueItem) != null)
                return true;
            this.arbitraryDataBuildQueue.notifyAll();
        }

        ArbitraryResourceStateRegistry.getInstance().invalidate(
                queueItem.getService(), queueItem.getResourceId(), queueItem.getIdentifier());

        log(queueItem, String.format("Added %s to build queue", queueItem));

        // Added to queue
        return true;
    }

    /** Wait for eligible work, then select and claim it without sorting or doing I/O. */
    ArbitraryDataBuildQueueItem takeNextBuild(boolean largeFilesOnly) throws InterruptedException {
        synchronized (this.arbitraryDataBuildQueue) {
            while (!this.isStopping && !Controller.isStopping()) {
                Long now = NTP.getTime();
                if (now == null) {
                    // Clock synchronization has no queue event; only this exceptional case polls.
                    this.arbitraryDataBuildQueue.wait(1000L);
                    continue;
                }
                ArbitraryDataBuildQueueItem next = null;
                long highestPriority = Long.MIN_VALUE;
                for (ArbitraryDataBuildQueueItem item : this.arbitraryDataBuildQueue.values()) {
                    if (!item.isQueued() || (largeFilesOnly && !item.isLargeFile()))
                        continue;
                    long priority = item.getEffectivePriority(now);
                    if (next == null || priority > highestPriority) {
                        next = item;
                        highestPriority = priority;
                    }
                }
                if (next != null) {
                    next.prepareForBuild(now);
                    return next;
                }
                this.arbitraryDataBuildQueue.wait();
            }
        }
        return null;
    }

    public void removeFromBuildQueue(ArbitraryDataBuildQueueItem queueItem) {
        if (queueItem == null)
            return;
        this.arbitraryDataBuildQueue.remove(queueItem.getUniqueKey(), queueItem);
        ArbitraryResourceStateRegistry.getInstance().invalidate(
                queueItem.getService(), queueItem.getResourceId(), queueItem.getIdentifier());
    }

    public boolean isInBuildQueue(ArbitraryDataBuildQueueItem queueItem) {
        String key = queueItem.getUniqueKey();
        if (key == null) {
            return false;
        }

        if (this.arbitraryDataBuildQueue == null) {
            return false;
        }

        if (this.arbitraryDataBuildQueue.containsKey(key)) {
            // Already in queue
            return true;
        }

        // Not in queue
        return false;
    }

    /**
     * Checks queue membership without allocating a temporary queue item on cache/search hot paths.
     */
    public boolean isInBuildQueue(String resourceId, ResourceIdType resourceIdType, Service service, String identifier) {
        String key = buildResourceKey(resourceId, resourceIdType, service, identifier);
        return key != null && this.arbitraryDataBuildQueue != null && this.arbitraryDataBuildQueue.containsKey(key);
    }


    // Failed builds

    public boolean addToFailedBuildsList(ArbitraryDataBuildQueueItem queueItem) {
        String key = queueItem.getUniqueKey();
        if (key == null) {
            return false;
        }

        if (this.arbitraryDataFailedBuilds == null) {
            return false;
        }

        if (NTP.getTime() == null) {
            // Can't use queues until we have synced the time
            return false;
        }

        if (this.arbitraryDataFailedBuilds.put(key, queueItem) != null) {
            // Already in list
            return true;
        }

        log(queueItem, String.format("Added %s to failed builds list", queueItem));

        // Added to queue
        return true;
    }

    public boolean isInFailedBuildsList(ArbitraryDataBuildQueueItem queueItem) {
        String key = queueItem.getUniqueKey();
        if (key == null) {
            return false;
        }

        if (this.arbitraryDataFailedBuilds == null) {
            return false;
        }

        if (this.arbitraryDataFailedBuilds.containsKey(key)) {
            // Already in list
            return true;
        }

        // Not in list
        return false;
    }

    public boolean isInFailedBuildsList(String resourceId, ResourceIdType resourceIdType, Service service,
                                        String identifier) {
        String key = buildResourceKey(resourceId, resourceIdType, service, identifier);
        return key != null && this.arbitraryDataFailedBuilds != null && this.arbitraryDataFailedBuilds.containsKey(key);
    }

    private static String buildResourceKey(String resourceId, ResourceIdType resourceIdType, Service service,
                                           String identifier) {
        if (resourceId == null || resourceIdType == null || service == null)
            return null;
        if (identifier == null || identifier.isEmpty() || "default".equals(identifier))
            identifier = null;
        return String.format("%s-%s-%s", service, resourceId.toLowerCase(), identifier).toLowerCase();
    }


    public void setBuildInProgress(boolean buildInProgress) {
        if (buildInProgress) {
            this.buildsInProgress.incrementAndGet();
        } else {
            this.buildsInProgress.updateAndGet(count -> Math.max(0, count - 1));
        }
    }

    public boolean getBuildInProgress() {
        return this.buildsInProgress.get() > 0;
    }

    private void log(ArbitraryDataBuildQueueItem queueItem, String message) {
        if (queueItem == null) {
            return;
        }

        if (queueItem.isHighPriority()) {
            LOGGER.trace(message);
        }
        else {
            LOGGER.debug(message);
        }
    }
}
