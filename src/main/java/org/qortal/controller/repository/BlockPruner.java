package org.qortal.controller.repository;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.qortal.controller.Controller;
import org.qortal.controller.Synchronizer;
import org.qortal.data.block.BlockData;
import org.qortal.repository.BlockArchiveReader;
import org.qortal.repository.Repository;
import org.qortal.repository.RepositoryManager;
import org.qortal.settings.Settings;
import org.qortal.utils.NTP;

import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.TimeUnit;

public class BlockPruner implements Runnable {

	private static final Logger LOGGER = LogManager.getLogger(BlockPruner.class);

	@Override
	public void run() {
		Thread.currentThread().setName("Block pruner");

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
			pruneStartHeight = repository.getBlockRepository().getBlockPruneHeight();
		} catch (Exception e) {
			LOGGER.error("Block Pruning is not working! Not trying again. Restart ASAP. Report this error immediately to the developers.", e);
			return;
		}

		while (!Controller.isStopping()) {

			try (final Repository repository = RepositoryManager.getRepository()) {

				try {
					repository.discardChanges();

					Thread.sleep(Settings.getInstance().getBlockPruneInterval());

					BlockData chainTip = Controller.getInstance().getChainTip();
					if (chainTip == null || NTP.getTime() == null)
						continue;

					// Prune all blocks up until our latest minus pruneBlockLimit
					final int ourLatestHeight = chainTip.getHeight();
					int upperPrunableHeight = ourLatestHeight - Settings.getInstance().getPruneBlockLimit();

					// In archive mode require both the committed archive cursor and an
					// actual durable archive file before deleting any block rows.
					if (archiveMode) {
						int committedArchiveHeight = repository.getBlockArchiveRepository().getBlockArchiveHeight() - 1;
						int durableArchiveHeight = BlockArchiveReader.getInstance().getHeightOfLastArchivedBlock();
						upperPrunableHeight = Math.min(upperPrunableHeight,
								Math.min(committedArchiveHeight, durableArchiveHeight));
					}

						int pruneBatchSize = Settings.getInstance().getBlockPruneBatchSize();
						Long minimumLatestBlockTimestamp = Controller.getMinimumLatestBlockTimestamp();
						boolean historicalReplay = minimumLatestBlockTimestamp != null
								&& chainTip.getTimestamp() < minimumLatestBlockTimestamp;
						if (historicalReplay || Synchronizer.getInstance().isSynchronizing())
							pruneBatchSize = Math.min(pruneBatchSize, 1000);
					int upperBatchHeight = pruneStartHeight + pruneBatchSize;
					int upperPruneHeight = Math.min(upperBatchHeight, upperPrunableHeight);

					if (pruneStartHeight >= upperPruneHeight) {
						continue;
					}

					if (archiveMode) {
						int firstArchivedHeight = Math.max(2, pruneStartHeight);
						if (BlockArchiveReader.getInstance().fetchBlockAtHeight(firstArchivedHeight) == null
								|| BlockArchiveReader.getInstance().fetchBlockAtHeight(upperPruneHeight) == null) {
							LOGGER.warn("Refusing to prune block range {}-{} because its archive is unreadable",
									pruneStartHeight, upperPruneHeight);
							Thread.sleep(60 * 1000L);
							continue;
						}
					}

						repository.discardChanges();
						ReentrantLock chainLock = Controller.getInstance().getBlockchainLock();
						if (!chainLock.tryLock(60, TimeUnit.SECONDS))
							continue;

						int numBlocksPruned;
						int previousPruneHeight = pruneStartHeight;
						int nextPruneHeight = upperPruneHeight + 1;
						try {
							LOGGER.info(String.format("Pruning blocks between %d and %d...", pruneStartHeight, upperPruneHeight));
							numBlocksPruned = repository.getBlockRepository().pruneBlocks(pruneStartHeight, upperPruneHeight);
							// The DELETE has no row limit, so success completely covers this
							// inclusive range. Commit the first unpruned height atomically with it.
							repository.getBlockRepository().setBlockPruneHeight(nextPruneHeight);
							pruneStartHeight = nextPruneHeight;
						} finally {
							chainLock.unlock();
						}

						if (numBlocksPruned > 0) {
							LOGGER.info(String.format("Pruned %d block%s between %d and %d",
									numBlocksPruned, (numBlocksPruned != 1 ? "s" : ""),
									previousPruneHeight, upperPruneHeight));
						} else {
							LOGGER.info(String.format("Bumping block base prune height to %d", nextPruneHeight));
						}

						if (upperPrunableHeight <= nextPruneHeight) {
							// We've pruned up to the upper prunable height
							// Back off for a while to save CPU for syncing
							repository.discardChanges();
							Thread.sleep(10*60*1000L);
						}
				} catch (InterruptedException e) {
					if(Controller.isStopping()) {
						LOGGER.info("Block Pruning Shutting Down");
					}
					else {
						LOGGER.warn("Block Pruning interrupted. Trying again. Report this error immediately to the developers.", e);
					}
				} catch (Exception e) {
					LOGGER.warn("Block Pruning stopped working. Trying again. Report this error immediately to the developers.", e);
				}
			} catch(Exception e){
				LOGGER.error("Block Pruning is not working! Not trying again. Restart ASAP. Report this error immediately to the developers.", e);
			}
		}
	}
}
