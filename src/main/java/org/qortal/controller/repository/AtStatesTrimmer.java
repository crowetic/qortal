package org.qortal.controller.repository;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.qortal.controller.Controller;
import org.qortal.data.block.BlockData;
import org.qortal.repository.Repository;
import org.qortal.repository.RepositoryManager;
import org.qortal.repository.ATRepository;
import org.qortal.settings.Settings;
import org.qortal.utils.NTP;

import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.MIN_PRIORITY;

/** Trims old payload columns from height-keyed AT block bundles in bounded transactions. */
public class AtStatesTrimmer implements Runnable {

    private static final Logger LOGGER = LogManager.getLogger(AtStatesTrimmer.class);
	private static final int HISTORICAL_TRIM_BATCH_SIZE = 1_000;
	private static final int HISTORICAL_TRIM_PAYLOAD_LIMIT = 1_000;

    @Override
    public void run() {
        Thread.currentThread().setName("AT States trimmer");
		Thread.currentThread().setPriority(MIN_PRIORITY);

		if (Settings.getInstance().isLite())
			return;

		int trimStartHeight;
		try (Repository repository = RepositoryManager.getRepository()) {
			trimStartHeight = repository.getATRepository().getAtTrimHeight();
			repository.discardChanges();
		} catch (Exception e) {
			LOGGER.error("AT bundle trimming is not working; restart and report this error", e);
			return;
		}

		while (!Controller.isStopping()) {
			try (Repository repository = RepositoryManager.getRepository()) {
				try {
					repository.discardChanges();
					Thread.sleep(Settings.getInstance().getAtStatesTrimInterval());

					BlockData chainTip = Controller.getInstance().getChainTip();
					Long networkTime = NTP.getTime();
					if (chainTip == null || networkTime == null)
						continue;

					long lifetime = Settings.getInstance().getAtStatesMaxLifetime();
					long upperTimestamp = Math.min(networkTime - lifetime, chainTip.getTimestamp() - lifetime);
					int timeTrimmableHeight = repository.getBlockRepository().getHeightFromTimestamp(upperTimestamp);
					int blockTrimmableHeight = chainTip.getHeight() - Settings.getInstance().getPruneBlockLimit();
					int upperTrimmableHeight = Math.min(timeTrimmableHeight, blockTrimmableHeight);
					Long minimumLatestBlockTimestamp = Controller.getMinimumLatestBlockTimestamp();
					boolean historicalReplay = minimumLatestBlockTimestamp != null
							&& chainTip.getTimestamp() < minimumLatestBlockTimestamp;
					int trimBatchSize = Settings.getInstance().getAtStatesTrimBatchSize();
					if (historicalReplay)
						trimBatchSize = Math.max(trimBatchSize, HISTORICAL_TRIM_BATCH_SIZE);
					int upperBatchHeight = trimStartHeight + trimBatchSize - 1;
					int upperTrimHeight = Math.min(upperBatchHeight, upperTrimmableHeight);
					boolean hasCurrentRange = trimStartHeight <= upperTrimHeight;

					ReentrantLock chainLock = Controller.getInstance().getBlockchainLock();
					repository.discardChanges();
					if (!chainLock.tryLock(60, TimeUnit.SECONDS))
						continue;

					ATRepository.ATTrimResult trimResult;
					int previousTrimHeight = trimStartHeight;
					try {
						int trimLimit = Settings.getInstance().getAtStatesTrimLimit();
						if (historicalReplay)
							trimLimit = Math.min(trimLimit, HISTORICAL_TRIM_PAYLOAD_LIMIT);
						trimResult = repository.getATRepository().trimAtStatesDetailed(trimStartHeight, upperTrimHeight,
								trimLimit);
						if (hasCurrentRange && trimResult.isRangeComplete()) {
							trimStartHeight = upperTrimHeight + 1;
							repository.getATRepository().setAtTrimHeight(trimStartHeight);
						} else
							repository.saveChanges();
					} finally {
						chainLock.unlock();
					}

					if (trimResult.getTrimmed() > 0) {
						if (hasCurrentRange) {
							final int from = previousTrimHeight;
							LOGGER.debug(() -> String.format("Trimmed %d AT block bundle%s between blocks %d and %d",
									trimResult.getTrimmed(), trimResult.getTrimmed() == 1 ? "" : "s", from, upperTrimHeight));
						} else {
							final int caughtUpHeight = previousTrimHeight;
							LOGGER.debug(() -> String.format("Trimmed %d historical AT block bundle%s below caught-up height %d",
									trimResult.getTrimmed(), trimResult.getTrimmed() == 1 ? "" : "s", caughtUpHeight));
						}
					}
				} catch (InterruptedException e) {
					if (Controller.isStopping())
						LOGGER.info("AT bundle trimmer shutting down");
					else
						LOGGER.warn("AT bundle trimmer interrupted; retrying", e);
				} catch (Exception e) {
					LOGGER.warn("AT bundle trimming failed; retrying", e);
				}
			} catch (Exception e) {
				LOGGER.error("AT bundle trimming is not working; restart and report this error", e);
			}
		}
    }
}
