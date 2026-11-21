package org.qortal.controller.repository;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.qortal.controller.Controller;
import org.qortal.data.block.BlockData;
import org.qortal.repository.DataException;
import org.qortal.repository.Repository;
import org.qortal.repository.RepositoryManager;
import org.qortal.settings.Settings;
import org.qortal.utils.NTP;

import java.util.concurrent.TimeUnit;

import static java.lang.Thread.MIN_PRIORITY;

public class AtStatesPruner implements Runnable {

	private static final Logger LOGGER = LogManager.getLogger(AtStatesPruner.class);
	private static final int HISTORICAL_PRUNE_BATCH_SIZE = 1_000;

	@Override
	public void run() {
		Thread.currentThread().setName("AT States pruner");
		Thread.currentThread().setPriority(MIN_PRIORITY);

		if (Settings.getInstance().isLite()) {
			// Nothing to prune in lite mode
			return;
		}

		boolean archiveMode = false;
		if (!Settings.getInstance().isTopOnly()) {
			// Top-only mode isn't enabled, but we might want to prune for the purposes of archiving
			if (!Settings.getInstance().isArchiveEnabled()) {
				// No pruning or archiving, so we must not prune anything
				return;
			}
			else {
				// We're allowed to prune blocks that have already been archived
				archiveMode = true;
			}
		}

		int pruneStartHeight;

		try (final Repository repository = RepositoryManager.getRepository()) {
			pruneStartHeight = repository.getATRepository().getAtPruneHeight();
			repository.discardChanges();
		} catch (Exception e) {
			LOGGER.error("AT States Pruning is not working! Not trying again. Restart ASAP. Report this error immediately to the developers.", e);
			return;
		}

		while (!Controller.isStopping()) {
			try (final Repository repository = RepositoryManager.getRepository()) {

				try {
					repository.discardChanges();

					Thread.sleep(Settings.getInstance().getAtStatesPruneInterval());

					BlockData chainTip = Controller.getInstance().getChainTip();
					if (chainTip == null || NTP.getTime() == null)
						continue;

					// Prune AT states for all blocks up until our latest minus pruneBlockLimit
					final int ourLatestHeight = chainTip.getHeight();
					int upperPrunableHeight = ourLatestHeight - Settings.getInstance().getPruneBlockLimit();
					// A bundle is disposable only after both retention promises have expired:
					// the block-count rollback window and five days of chain time.
					long retainedTimestamp = chainTip.getTimestamp() - Settings.getInstance().getAtStatesMaxLifetime();
					int timePrunableHeight = repository.getBlockRepository().getHeightFromTimestamp(retainedTimestamp);
					upperPrunableHeight = Math.min(upperPrunableHeight, timePrunableHeight);

					// In archive mode we are only allowed to trim blocks that have already been archived
					if (archiveMode) {
						upperPrunableHeight = Math.min(upperPrunableHeight,
								repository.getBlockArchiveRepository().getBlockArchiveHeight() - 1);

						// TODO: validate that the actual archived data exists before pruning it?
					}

					Long minimumLatestBlockTimestamp = Controller.getMinimumLatestBlockTimestamp();
					boolean historicalReplay = minimumLatestBlockTimestamp != null
							&& chainTip.getTimestamp() < minimumLatestBlockTimestamp;
					int pruneBatchSize = Settings.getInstance().getAtStatesPruneBatchSize();
					if (historicalReplay)
						pruneBatchSize = Math.max(pruneBatchSize, HISTORICAL_PRUNE_BATCH_SIZE);
					int upperBatchHeight = pruneStartHeight + pruneBatchSize - 1;
					int upperPruneHeight = Math.min(upperBatchHeight, upperPrunableHeight);

					if (pruneStartHeight > upperPruneHeight)
						continue;

					LOGGER.debug(String.format("Pruning AT states between blocks %d and %d...", pruneStartHeight, upperPruneHeight));

					java.util.concurrent.locks.ReentrantLock chainLock = Controller.getInstance().getBlockchainLock();
					repository.discardChanges();
					if (!chainLock.tryLock(60, TimeUnit.SECONDS))
						continue;
					int numAtStatesPruned;
					int previousPruneHeight = pruneStartHeight;
					int nextPruneHeight = upperPruneHeight + 1;
					try {
						numAtStatesPruned = repository.getATRepository().pruneAtStates(pruneStartHeight, upperPruneHeight);
						// The DELETE completely examines this inclusive range. Commit the first
						// unprocessed height atomically with it, even when rows were deleted.
						repository.getATRepository().setAtPruneHeight(nextPruneHeight);
						pruneStartHeight = nextPruneHeight;
					} finally {
						chainLock.unlock();
					}

					if (numAtStatesPruned > 0) {
						final int finalPruneStartHeight = previousPruneHeight;
						LOGGER.debug(() -> String.format("Pruned %d AT state%s between blocks %d and %d",
								numAtStatesPruned, (numAtStatesPruned != 1 ? "s" : ""),
								finalPruneStartHeight, upperPruneHeight));
					} else {
						final int finalPruneHeight = pruneStartHeight;
						LOGGER.debug(() -> String.format("Bumping AT state base prune height to %d", finalPruneHeight));
					}

					if (pruneStartHeight > upperPrunableHeight) {
						repository.discardChanges();
						Thread.sleep(5 * 60 * 1000L);
					}
				} catch (InterruptedException e) {
					if (Controller.isStopping()) {
						LOGGER.info("AT States Pruning Shutting Down");
					} else {
						LOGGER.warn("AT States Pruning interrupted. Trying again. Report this error immediately to the developers.", e);
					}
				} catch (Exception e) {
					LOGGER.warn("AT States Pruning stopped working. Trying again. Report this error immediately to the developers.", e);
				}
			} catch(Exception e){
				LOGGER.error("AT States Pruning is not working! Not trying again. Restart ASAP. Report this error immediately to the developers.", e);
			}
		}
	}
}
