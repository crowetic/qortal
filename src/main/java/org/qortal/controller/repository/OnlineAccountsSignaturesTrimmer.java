package org.qortal.controller.repository;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.qortal.block.BlockChain;
import org.qortal.controller.Controller;
import org.qortal.data.block.BlockData;
import org.qortal.repository.Repository;
import org.qortal.repository.RepositoryManager;
import org.qortal.settings.Settings;
import org.qortal.utils.NTP;

import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.TimeUnit;

public class OnlineAccountsSignaturesTrimmer implements Runnable {

	private static final Logger LOGGER = LogManager.getLogger(OnlineAccountsSignaturesTrimmer.class);

	private static final long INITIAL_SLEEP_PERIOD = 5 * 60 * 1000L + 1234L; // ms
	private static final int HISTORICAL_TRIM_BATCH_SIZE = 5_000;

	public void run() {
		Thread.currentThread().setName("Online Accounts trimmer");

		if (Settings.getInstance().isLite()) {
			// Nothing to trim in lite mode
			return;
		}

		int trimStartHeight;

		try (final Repository repository = RepositoryManager.getRepository()) {
			// Don't even start trimming until initial rush has ended
			Thread.sleep(INITIAL_SLEEP_PERIOD);

			trimStartHeight = repository.getBlockRepository().getOnlineAccountsSignaturesTrimHeight();
		} catch (InterruptedException e) {
			if (Controller.isStopping()) {
				LOGGER.info("Online Accounts Signatures Trimming shutting down");
			} else {
				LOGGER.error("Online Accounts Signatures Trimming interrupted during initialization. Restart ASAP. Report this error immediately to the developers.", e);
			}
			return;
		} catch (Exception e) {
			LOGGER.error("Online Accounts Signatures Trimming is not working! Not trying again. Restart ASAP. Report this error immediately to the developers.", e);
			return;
		}

		while (!Controller.isStopping()) {
			try (final Repository repository = RepositoryManager.getRepository()) {

				try {
					repository.discardChanges();

					Thread.sleep(Settings.getInstance().getOnlineSignaturesTrimInterval());

					BlockData chainTip = Controller.getInstance().getChainTip();
					if (chainTip == null || NTP.getTime() == null)
						continue;

					// Keep both the chain-time retention period and the block-count rollback window.
					long signaturesLifetime = BlockChain.getInstance().getOnlineAccountSignaturesMaxLifetime();
					long upperTrimmableTimestamp = Math.min(NTP.getTime() - signaturesLifetime,
							chainTip.getTimestamp() - signaturesLifetime);
					int timeTrimmableHeight = repository.getBlockRepository().getHeightFromTimestamp(upperTrimmableTimestamp);
					int blockTrimmableHeight = chainTip.getHeight() - Settings.getInstance().getPruneBlockLimit();
					int upperTrimmableHeight = Math.min(timeTrimmableHeight, blockTrimmableHeight);

					Long minimumLatestBlockTimestamp = Controller.getMinimumLatestBlockTimestamp();
					boolean historicalReplay = minimumLatestBlockTimestamp != null
							&& chainTip.getTimestamp() < minimumLatestBlockTimestamp;
					int trimBatchSize = Settings.getInstance().getOnlineSignaturesTrimBatchSize();
					if (historicalReplay)
						trimBatchSize = Math.max(trimBatchSize, HISTORICAL_TRIM_BATCH_SIZE);
					int upperBatchHeight = trimStartHeight + trimBatchSize;
					int upperTrimHeight = Math.min(upperBatchHeight, upperTrimmableHeight);

					if (trimStartHeight >= upperTrimHeight)
						continue;

					ReentrantLock chainLock = Controller.getInstance().getBlockchainLock();
					// Sync owns this lock for an entire batch. Release our read snapshot,
					// then queue across a full batch so maintenance cannot starve.
					repository.discardChanges();
					if (!chainLock.tryLock(60, TimeUnit.SECONDS))
						continue;

					int numSigsTrimmed;
					int previousTrimHeight = trimStartHeight;
					try {
						numSigsTrimmed = repository.getBlockRepository()
								.trimOldOnlineAccountsSignatures(previousTrimHeight, upperTrimHeight);
						// The UPDATE has no row limit, so a successful call completely covers
						// this inclusive range. Publish the first untrimmed height atomically
						// with the row changes instead of scanning the range a second time.
						trimStartHeight = upperTrimHeight + 1;
						repository.getBlockRepository().setOnlineAccountsSignaturesTrimHeight(trimStartHeight);
					} finally {
						chainLock.unlock();
					}

					if (numSigsTrimmed > 0) {
						final int trimmedFrom = previousTrimHeight;
						LOGGER.info(() -> String.format("Trimmed %d online accounts signature%s between blocks %d and %d",
								numSigsTrimmed, (numSigsTrimmed != 1 ? "s" : ""), trimmedFrom, upperTrimHeight));
					}
				} catch (InterruptedException e) {
					if(Controller.isStopping()) {
						LOGGER.info("Online Accounts Signatures Trimming Shutting Down");
					}
					else {
						LOGGER.warn("Online Accounts Signatures Trimming interrupted. Trying again. Report this error immediately to the developers.", e);
					}
				} catch (Exception e) {
					LOGGER.warn("Online Accounts Signatures Trimming stopped working. Trying again. Report this error immediately to the developers.", e);
				}
			} catch (Exception e) {
				LOGGER.error("Online Accounts Signatures Trimming is not working! Not trying again. Restart ASAP. Report this error immediately to the developers.", e);
			}
		}
	}
}
