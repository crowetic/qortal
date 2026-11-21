package org.qortal.controller;

import com.google.common.hash.HashCode;
import org.junit.Before;
import org.junit.Test;
import org.qortal.account.PrivateKeyAccount;
import org.qortal.crosschain.BitcoinACCTv3;
import org.qortal.data.block.BlockData;
import org.qortal.repository.DataException;
import org.qortal.repository.Repository;
import org.qortal.repository.RepositoryManager;
import org.qortal.test.common.AtUtils;
import org.qortal.test.common.BlockUtils;
import org.qortal.test.common.Common;
import org.qortal.transaction.DeployAtTransaction;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CrossChainOfferIndexTests extends Common {

	private CrossChainOfferIndex index;

	@Before
	public void beforeTest() throws DataException {
		Common.useDefaultSettings();
		this.index = CrossChainOfferIndex.getInstance();
	}

	@Test
	public void startupSnapshotContainsCurrentOfferAndCreatorKey() throws Exception {
		String atAddress;
		byte[] creatorPublicKey;
		BlockData parentTip;
		this.index.processPendingSafely();
		try (Repository repository = RepositoryManager.getRepository()) {
			parentTip = repository.getBlockRepository().getLastBlock();
		}

		try (Repository repository = RepositoryManager.getRepository()) {
			PrivateKeyAccount deployer = Common.getTestAccount(repository, "alice");
			PrivateKeyAccount tradeAccount = Common.getTestAccount(repository, "bob");
			byte[] foreignPublicKeyHash = HashCode.fromString(
					"bb00bb11bb22bb33bb44bb55bb66bb77bb88bb99").asBytes();
			byte[] creationBytes = BitcoinACCTv3.buildQortalAT(tradeAccount.getAddress(), foreignPublicKeyHash,
					10_00000000L, 1_00000000L, 1440);
			DeployAtTransaction deployment = AtUtils.doDeployAT(repository, deployer, creationBytes, 12_00000000L);
			atAddress = deployment.getATAccount().getAddress();
			creatorPublicKey = deployer.getPublicKey();
		}

		try (Repository repository = RepositoryManager.getRepository()) {
			this.index.listen(new Controller.NewBlockEvent(repository.getBlockRepository().getLastBlock()));
		}
		this.index.processPendingSafely();

		assertTrue(this.index.getSnapshot().isReady());
		CrossChainOfferIndex.Offer offer = this.index.getOffer(atAddress);
		assertNotNull(offer);
		assertArrayEquals(creatorPublicKey, offer.getCreatorPublicKey());

		try (Repository repository = RepositoryManager.getRepository()) {
			BlockUtils.orphanLastBlock(repository);
		}
		this.index.listen(new Controller.OrphanedBlockEvent(parentTip));
		this.index.processPendingSafely();
		assertTrue(this.index.getOffer(atAddress) == null);
		assertArrayEquals(parentTip.getSignature(), this.index.getSnapshot().getSignature());
	}
}
