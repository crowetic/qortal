package org.qortal.controller.repository;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.qortal.controller.Controller;
import org.qortal.data.block.BlockData;
import org.qortal.repository.BlockArchiveReader;
import org.qortal.repository.BlockArchiveWriter;
import org.qortal.repository.DataException;
import org.qortal.repository.Repository;
import org.qortal.repository.RepositoryManager;
import org.qortal.settings.Settings;
import org.qortal.transform.TransformationException;

import java.io.IOException;
import java.nio.file.Files;

public class BlockArchiver implements Runnable {

	private static final Logger LOGGER = LogManager.getLogger(BlockArchiver.class);

	private static final long INITIAL_SLEEP_PERIOD = 15 * 60 * 1000L; // ms
	private static final long HISTORICAL_REPLAY_BLOCK_PAUSE = 25L; // ms

	public void run() {
		Thread.currentThread().setName("Block archiver");

		if (!Settings.getInstance().isArchiveEnabled() || Settings.getInstance().isLite()) {
			return;
		}

		int startHeight;

		try (final Repository repository = RepositoryManager.getRepository()) {
			// Don't even start building until initial rush has ended
			Thread.sleep(INITIAL_SLEEP_PERIOD);

			startHeight = repository.getBlockArchiveRepository().getBlockArchiveHeight();
		} catch (InterruptedException e) {
			if (Controller.isStopping()) {
				LOGGER.info("Block Archiving shutting down");
			} else {
				LOGGER.error("Block Archiving interrupted during initialization. Restart ASAP. Report this error immediately to the developers.", e);
			}
			return;
		} catch (Exception e) {
			LOGGER.error("Block Archiving is not working! Not trying again. Restart ASAP. Report this error immediately to the developers.", e);
			return;
		}

		LOGGER.info("Starting block archiver from height {}...", startHeight);

		while (!Controller.isStopping()) {
			try (final Repository repository = RepositoryManager.getRepository()) {

				try {
					repository.discardChanges();

					Thread.sleep(Settings.getInstance().getArchiveInterval());

					BlockData chainTip = Controller.getInstance().getChainTip();
					if (chainTip == null) {
						continue;
					}

					// Archive only data that both trimmers have released, with an
					// independent rollback-window cap as defense in depth.
					try {
						final int rollbackSafeHeight = chainTip.getHeight() - Settings.getInstance().getPruneBlockLimit();
						final int maximumArchiveHeight = Math.min(
								BlockArchiveWriter.getMaxArchiveHeight(repository), rollbackSafeHeight);
						BlockArchiveWriter writer = new BlockArchiveWriter(startHeight, maximumArchiveHeight, repository);
						Long minimumLatestBlockTimestamp = Controller.getMinimumLatestBlockTimestamp();
						boolean historicalReplay = minimumLatestBlockTimestamp != null
								&& chainTip.getTimestamp() < minimumLatestBlockTimestamp;
						if (historicalReplay)
							writer.setBlockPause(HISTORICAL_REPLAY_BLOCK_PAUSE);
						BlockArchiveWriter.BlockArchiveWriteResult result = writer.write();
						switch (result) {
							case OK:
								// Re-open the published file through the normal reader before
								// committing its index rows and advancing the pruning boundary.
								if (BlockArchiveReader.getInstance().fetchBlockAtHeight(writer.getLastWrittenHeight()) == null) {
									repository.discardChanges();
									Files.deleteIfExists(writer.getOutputPath());
									BlockArchiveReader.getInstance().invalidateFileListCache();
									LOGGER.warn("Refusing to publish unreadable block archive {}", writer.getOutputPath());
									break;
								}
								startHeight = writer.getLastWrittenHeight() + 1;
								repository.getBlockArchiveRepository().setBlockArchiveHeight(startHeight);
								break;

							case STOPPING:
								return;

								// We've reached the limit of the blocks we can archive
								// Sleep for a while to allow more to become available
							case NOT_ENOUGH_BLOCKS:
								// During historical replay, check often enough to keep the live
								// database bounded. A caught-up node can retain the old backoff.
								repository.discardChanges();
								Thread.sleep(historicalReplay ? 10 * 60 * 1000L : 2 * 60 * 60 * 1000L);
								break;

							case BLOCK_NOT_FOUND:
								// We tried to archive a block that didn't exist. This is a major failure and likely means
								// that a bootstrap or re-sync is needed. Try again every minute until then.
								LOGGER.info("Error: block not found when building archive. If this error persists, " +
										"a bootstrap or re-sync may be needed.");
								repository.discardChanges();
								Thread.sleep(60 * 1000L); // 1 minute
								break;
						}

					} catch (IOException | TransformationException e) {
						LOGGER.info("Caught exception when creating block cache", e);
					}
				} catch (InterruptedException e) {
					if (Controller.isStopping()) {
						LOGGER.info("Block Archiving Shutting Down");
					} else {
						LOGGER.warn("Block Archiving interrupted. Trying again. Report this error immediately to the developers.", e);
					}
				} catch (Exception e) {
					LOGGER.warn("Block Archiving stopped working. Trying again. Report this error immediately to the developers.", e);
				}
			} catch(Exception e){
				LOGGER.error("Block Archiving is not working! Not trying again. Restart ASAP. Report this error immediately to the developers.", e);
			}
		}
	}
}
