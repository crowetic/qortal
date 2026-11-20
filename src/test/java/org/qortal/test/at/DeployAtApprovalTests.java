package org.qortal.test.at;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.ciyam.at.MachineState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.qortal.account.PrivateKeyAccount;
import org.qortal.asset.Asset;
import org.qortal.at.QortalATAPI;
import org.qortal.at.QortalAtLoggerFactory;
import org.qortal.block.BlockChain;
import org.qortal.crosschain.BitcoinACCTv3;
import org.qortal.crosschain.DigibyteACCTv3;
import org.qortal.crosschain.DogecoinACCTv3;
import org.qortal.crosschain.LitecoinACCTv1;
import org.qortal.crosschain.LitecoinACCTv3;
import org.qortal.crosschain.PirateChainACCTv3;
import org.qortal.crosschain.RavencoinACCTv3;
import org.qortal.crosschain.SupportedBlockchain;
import org.qortal.data.at.ATData;
import org.qortal.data.transaction.BaseTransactionData;
import org.qortal.data.transaction.DeployAtTransactionData;
import org.qortal.group.Group;
import org.qortal.repository.Repository;
import org.qortal.repository.RepositoryManager;
import org.qortal.test.common.AtUtils;
import org.qortal.test.common.Common;
import org.qortal.transaction.DeployAtTransaction;
import org.qortal.transaction.Transaction.ValidationResult;

import java.util.Map;

import static org.junit.Assert.*;

/** Characterize the real deployment gate before a future XMR consensus change. */
public class DeployAtApprovalTests extends Common {
	private Map<String, Long> triggers;
	private Long originalTrigger;
	private static final String TRIGGER = BlockChain.FeatureTrigger.atValidateHeight.name();

	@Before
	@SuppressWarnings("unchecked")
	public void beforeTest() throws Exception {
		Common.useDefaultSettings();
		triggers = (Map<String, Long>) FieldUtils.readField(BlockChain.getInstance(), "featureTriggers", true);
		originalTrigger = triggers.get(TRIGGER);
	}

	@After
	public void restoreTrigger() {
		if (triggers != null && originalTrigger != null)
			triggers.put(TRIGGER, originalTrigger);
	}

	@Test
	public void unknownCodeIsAllowedBeforeActivation() throws Exception {
		try (Repository repository = RepositoryManager.getRepository()) {
			triggers.put(TRIGGER, (long) nextHeight(repository) + 1);
			assertEquals(ValidationResult.OK, transaction(repository, AtUtils.buildSimpleAT(), "Test", 1L).isValid());
		}
	}

	@Test
	public void activationComparisonIsStrictlyGreaterThan() throws Exception {
		try (Repository repository = RepositoryManager.getRepository()) {
			triggers.put(TRIGGER, (long) nextHeight(repository));
			assertEquals(ValidationResult.OK, transaction(repository, AtUtils.buildSimpleAT(), "Test", 1L).isValid());
		}
	}

	@Test
	public void unknownCodeIsRejectedAfterActivation() throws Exception {
		try (Repository repository = RepositoryManager.getRepository()) {
			triggers.put(TRIGGER, (long) nextHeight(repository) - 1);
			assertEquals(ValidationResult.INVALID_CREATION_BYTES,
					transaction(repository, AtUtils.buildSimpleAT(), "Test", 1L).isValid());
		}
	}

	@Test
	public void metadataCannotAuthorizeANewContract() throws Exception {
		try (Repository repository = RepositoryManager.getRepository()) {
			triggers.put(TRIGGER, 0L);
			for (String label : new String[] { "MoneroACCTv1", "LitecoinACCTv3", "PirateChainACCTv3" })
				assertEquals(label, ValidationResult.INVALID_CREATION_BYTES,
						transaction(repository, AtUtils.buildSimpleAT(), label, 1L).isValid());
		}
	}

	@Test
	public void backdatingDoesNotBypassCandidateBlockHeight() throws Exception {
		try (Repository repository = RepositoryManager.getRepository()) {
			triggers.put(TRIGGER, 0L);
			assertEquals(ValidationResult.INVALID_CREATION_BYTES,
					transaction(repository, AtUtils.buildSimpleAT(), "Test", 1L).isValid());
			assertEquals(ValidationResult.INVALID_CREATION_BYTES,
					transaction(repository, AtUtils.buildSimpleAT(), "Test", System.currentTimeMillis()).isValid());
		}
	}

	@Test
	public void latestExistingTradeProgramsRemainApproved() throws Exception {
		try (Repository repository = RepositoryManager.getRepository()) {
			triggers.put(TRIGGER, 0L);
			String address = Common.getTestAccount(repository, "alice").getAddress();
			byte[][] programs = {
				BitcoinACCTv3.buildQortalAT(address, new byte[20], 100_00000000L, 100000L, 60),
				LitecoinACCTv3.buildQortalAT(address, new byte[20], 100_00000000L, 100000L, 60),
				DogecoinACCTv3.buildQortalAT(address, new byte[20], 100_00000000L, 100000L, 60),
				DigibyteACCTv3.buildQortalAT(address, new byte[20], 100_00000000L, 100000L, 60),
				RavencoinACCTv3.buildQortalAT(address, new byte[20], 100_00000000L, 100000L, 60),
				PirateChainACCTv3.buildQortalAT(address, new byte[33], 100_00000000L, 100000L, 60)
			};
			for (byte[] program : programs)
				assertEquals(ValidationResult.OK, transaction(repository, program, "Test", 1L).isValid());
		}
	}

	@Test
	public void historicalRecognitionDoesNotAuthorizeNewDeployment() throws Exception {
		try (Repository repository = RepositoryManager.getRepository()) {
			assertNotNull(SupportedBlockchain.getAcctByCodeHash(LitecoinACCTv1.CODE_BYTES_HASH));
			String address = Common.getTestAccount(repository, "alice").getAddress();
			byte[] program = LitecoinACCTv1.buildQortalAT(address, new byte[20], 100_00000000L, 100000L, 60);
			triggers.put(TRIGGER, (long) nextHeight(repository) + 1);
			assertEquals(ValidationResult.OK, transaction(repository, program, "LitecoinACCTv1", 1L).isValid());
			triggers.put(TRIGGER, 0L);
			assertEquals(ValidationResult.INVALID_CREATION_BYTES,
					transaction(repository, program, "LitecoinACCTv1", 1L).isValid());
		}
	}

	@Test
	public void approvalIsForCodeRatherThanOfferDataOrMetadata() throws Exception {
		try (Repository repository = RepositoryManager.getRepository()) {
			triggers.put(TRIGGER, 0L);
			for (String account : new String[] { "alice", "bob" }) {
				String address = Common.getTestAccount(repository, account).getAddress();
				byte[] program = LitecoinACCTv3.buildQortalAT(address, new byte[20], 321_00000000L, 500000L, 90);
				assertEquals(ValidationResult.OK, transaction(repository, program, "Different label", 1L).isValid());
			}
		}
	}

	@Test
	public void sizeLimitsApplyEvenToApprovedCode() throws Exception {
		try (Repository repository = RepositoryManager.getRepository()) {
			String address = Common.getTestAccount(repository, "alice").getAddress();
			byte[] program = LitecoinACCTv3.buildQortalAT(address, new byte[20], 100_00000000L, 100000L, 60);
			DeployAtTransaction tx = transaction(repository, program, "Test", 1L);
			DeployAtTransactionData data = (DeployAtTransactionData) tx.getTransactionData();
			DeployAtTransaction.ensureATAddress(data);
			ATData skeleton = new ATData(data.getAtAddress(), data.getCreatorPublicKey(), data.getTimestamp(), Asset.QORT);
			MachineState state = new MachineState(new QortalATAPI(repository, skeleton, 0),
					QortalAtLoggerFactory.getInstance(), program);
			byte[] oversized = MachineState.toCreationBytes((short) 2, state.getCodeBytes(),
					new byte[DeployAtTransaction.MAX_AT_STATE_LENGTH], (short) 0, (short) 0, 0L);
			for (long trigger : new long[] { 0L, nextHeight(repository) + 1L }) {
				triggers.put(TRIGGER, trigger);
				assertEquals(ValidationResult.INVALID_CREATION_BYTES,
						transaction(repository, oversized, "Test", 1L).isValid());
			}
		}
	}

	@Test
	public void walletSupportDoesNotRegisterMoneroTrading() {
		assertNull(SupportedBlockchain.fromString("MONERO"));
		assertNull(SupportedBlockchain.fromString("XMR"));
		assertNull(SupportedBlockchain.getAcctByName("MoneroACCTv1"));
		assertTrue(SupportedBlockchain.getFilteredAcctMap("MONERO").isEmpty());
	}

	private int nextHeight(Repository repository) throws Exception {
		return repository.getBlockRepository().getBlockchainHeight() + 1;
	}

	private DeployAtTransaction transaction(Repository repository, byte[] creation, String label, long timestamp) throws Exception {
		PrivateKeyAccount creator = Common.getTestAccount(repository, "alice");
		BaseTransactionData base = new BaseTransactionData(timestamp, Group.NO_GROUP, creator.getLastReference(),
				creator.getPublicKey(), 0L, null);
		DeployAtTransactionData data = new DeployAtTransactionData(base, label, label, label, label,
				creation, 1_00000000L, Asset.QORT);
		DeployAtTransaction transaction = new DeployAtTransaction(repository, data);
		data.setFee(transaction.calcRecommendedFee());
		return transaction;
	}
}
