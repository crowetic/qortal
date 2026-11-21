package org.qortal.controller.arbitrary;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.qortal.arbitrary.ArbitraryDataBuildQueueItem;
import org.qortal.arbitrary.exception.MissingDataException;
import org.qortal.controller.Controller;
import org.qortal.data.arbitrary.ArbitraryResourceStatus;
import org.qortal.repository.DataException;
import org.qortal.utils.ArbitraryTransactionUtils;

import java.io.IOException;

import static java.lang.Thread.NORM_PRIORITY;
import static org.qortal.data.arbitrary.ArbitraryResourceStatus.Status.NOT_PUBLISHED;


public class ArbitraryDataBuilderThread implements Runnable {

    private static final Logger LOGGER = LogManager.getLogger(ArbitraryDataBuilderThread.class);

    // Whether this thread is reserved for large files
    private final boolean isLargeFileThread;

    public ArbitraryDataBuilderThread() {
        this(false);
    }

    public ArbitraryDataBuilderThread(boolean isLargeFileThread) {
        this.isLargeFileThread = isLargeFileThread;
    }

    @Override
    public void run() {
        String threadName = isLargeFileThread ? "Arbitrary Data Builder Thread (Large Files)" : "Arbitrary Data Builder Thread";
        Thread.currentThread().setName(threadName);
        Thread.currentThread().setPriority(NORM_PRIORITY);
        ArbitraryDataBuildManager buildManager = ArbitraryDataBuildManager.getInstance();

        while (!Controller.isStopping()) {
            try {
                ArbitraryDataBuildQueueItem queueItem = buildManager.takeNextBuild(this.isLargeFileThread);
                if (queueItem == null)
                    return;

                try {
                    if (buildManager.isInFailedBuildsList(queueItem))
                        continue;

                    // Status reconciliation can wait on repository and resource-state work. Never do it
                    // while holding the queue lock: status requests can also inspect this queue.
                    ArbitraryResourceStatus arbitraryResourceStatus = ArbitraryTransactionUtils.getStatus(
                            queueItem.getService(), queueItem.getResourceId(), queueItem.getIdentifier(), false, true);
                    if (arbitraryResourceStatus == null) {
                        throw new DataException("Unable to determine resource status before build");
                    }
                    if (arbitraryResourceStatus.getStatus() == NOT_PUBLISHED) {
                        continue;
                    }

                    // Perform the build
 
                    queueItem.build();
                    log(queueItem, String.format("Finished building %s", queueItem));

                } catch (MissingDataException e) {
                    log(queueItem, String.format("Missing data for %s: %s", queueItem, e.getMessage()));
                    queueItem.setFailed(true);
                    // Don't add to the failed builds list, as we may want to retry sooner

                } catch (IOException | DataException | RuntimeException e) {
                    log(queueItem, String.format("Error building %s: %s", queueItem, e.getMessage()));
                    // Something went wrong - so remove it from the queue, and add to failed builds list
                    queueItem.setFailed(true);
                    buildManager.addToFailedBuildsList(queueItem);
                } finally {
                    // Release only this claim, including after status errors or interrupted builds.
                    buildManager.removeFromBuildQueue(queueItem);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void log(ArbitraryDataBuildQueueItem queueItem, String message) {
        if (queueItem == null) {
            return;
        }

        if (queueItem.isHighPriority()) {
            LOGGER.info(message);
        }
        else {
            LOGGER.debug(message);
        }
    }
}
