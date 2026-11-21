package org.qortal.test.at;

import org.ciyam.at.MachineState;
import org.junit.Before;
import org.junit.Test;
import org.qortal.account.PrivateKeyAccount;
import org.qortal.data.at.ATData;
import org.qortal.data.at.ATStateData;
import org.qortal.repository.DataException;
import org.qortal.repository.Repository;
import org.qortal.repository.RepositoryManager;
import org.qortal.test.common.AtUtils;
import org.qortal.test.common.BlockUtils;
import org.qortal.test.common.Common;
import org.qortal.transaction.DeployAtTransaction;

import java.util.List;
import java.util.Collections;

import org.qortal.utils.ByteArray;

import static org.junit.Assert.*;

public class AtRepositoryTests extends Common {

	@Before
	public void beforeTest() throws DataException {
		Common.useDefaultSettings();
	}

	@Test
	public void testGetATsByFunctionalitySinceCurrentHead() throws DataException {
		byte[] creationBytes = AtUtils.buildSimpleAT();

		try (final Repository repository = RepositoryManager.getRepository()) {
			PrivateKeyAccount deployer = Common.getTestAccount(repository, "alice");
			DeployAtTransaction deployment = AtUtils.doDeployAT(repository, deployer, creationBytes, 1_00000000L);
			BlockUtils.mintBlock(repository);

			ATData deployed = repository.getATRepository().fromATAddress(deployment.getATAccount().getAddress());
			int currentHeight = repository.getBlockRepository().getBlockchainHeight();

			List<ATData> atCurrentHeight = repository.getATRepository().getAllATsByFunctionalitySince(
					Collections.singleton(ByteArray.wrap(deployed.getCodeHash())), null, currentHeight);
			assertTrue(atCurrentHeight.stream().anyMatch(data -> data.getATAddress().equals(deployed.getATAddress())));

			List<ATData> beyondCurrentHeight = repository.getATRepository().getAllATsByFunctionalitySince(
					Collections.singleton(ByteArray.wrap(deployed.getCodeHash())), null, currentHeight + 1);
			assertTrue(beyondCurrentHeight.isEmpty());
		}
	}

	@Test
	public void testGetATStateAtHeightWithData() throws DataException {
		byte[] creationBytes = AtUtils.buildSimpleAT();

		try (final Repository repository = RepositoryManager.getRepository()) {
			PrivateKeyAccount deployer = Common.getTestAccount(repository, "alice");

			long fundingAmount = 1_00000000L;
			DeployAtTransaction deployAtTransaction = AtUtils.doDeployAT(repository, deployer, creationBytes, fundingAmount);
			String atAddress = deployAtTransaction.getATAccount().getAddress();

			for (int i = 0; i < 10; ++i)
				BlockUtils.mintBlock(repository);

			Integer testHeight = 8;
			ATStateData atStateData = repository.getATRepository().getATStateAtHeight(atAddress, testHeight);

			assertEquals(testHeight, atStateData.getHeight());
			assertNotNull(atStateData.getStateData());
			repository.discardChanges();
		}
	}

	@Test
	public void testGetLatestATStateWithData() throws DataException {
		byte[] creationBytes = AtUtils.buildSimpleAT();

		try (final Repository repository = RepositoryManager.getRepository()) {
			PrivateKeyAccount deployer = Common.getTestAccount(repository, "alice");

			long fundingAmount = 1_00000000L;
			DeployAtTransaction deployAtTransaction = AtUtils.doDeployAT(repository, deployer, creationBytes, fundingAmount);
			String atAddress = deployAtTransaction.getATAccount().getAddress();

			for (int i = 0; i < 10; ++i)
				BlockUtils.mintBlock(repository);
			int blockchainHeight = repository.getBlockRepository().getBlockchainHeight();

			Integer testHeight = blockchainHeight;
			ATStateData atStateData = repository.getATRepository().getLatestATState(atAddress);

			assertEquals(testHeight, atStateData.getHeight());
			assertNotNull(atStateData.getStateData());
			repository.discardChanges();
		}
	}

	@Test
	public void testGetMatchingFinalATStatesWithoutDataValue() throws DataException {
		byte[] creationBytes = AtUtils.buildSimpleAT();

		try (final Repository repository = RepositoryManager.getRepository()) {
			PrivateKeyAccount deployer = Common.getTestAccount(repository, "alice");

			long fundingAmount = 1_00000000L;
			DeployAtTransaction deployAtTransaction = AtUtils.doDeployAT(repository, deployer, creationBytes, fundingAmount);
			String atAddress = deployAtTransaction.getATAccount().getAddress();

			for (int i = 0; i < 10; ++i)
				BlockUtils.mintBlock(repository);
			int blockchainHeight = repository.getBlockRepository().getBlockchainHeight();

			Integer testHeight = blockchainHeight;

			ATData atData = repository.getATRepository().fromATAddress(atAddress);

			byte[] codeHash = atData.getCodeHash();
			Boolean isFinished = Boolean.FALSE;
			Integer dataByteOffset = null;
			Long expectedValue = null;
			Integer minimumFinalHeight = null;
			Integer limit = null;
			Integer offset = null;
			Boolean reverse = null;

			List<ATStateData> atStates = repository.getATRepository().getMatchingFinalATStates(
					codeHash,
					null,
					null,
					isFinished,
					dataByteOffset,
					expectedValue,
					minimumFinalHeight,
					limit, offset, reverse);

			assertEquals(false, atStates.isEmpty());
			assertEquals(1, atStates.size());

			ATStateData atStateData = atStates.get(0);
			assertEquals(testHeight, atStateData.getHeight());
			assertNotNull(atStateData.getStateData());
		}
	}

	@Test
	public void testGetMatchingFinalATStatesWithDataValue() throws DataException {
		byte[] creationBytes = AtUtils.buildSimpleAT();

		try (final Repository repository = RepositoryManager.getRepository()) {
			PrivateKeyAccount deployer = Common.getTestAccount(repository, "alice");

			long fundingAmount = 1_00000000L;
			DeployAtTransaction deployAtTransaction = AtUtils.doDeployAT(repository, deployer, creationBytes, fundingAmount);
			String atAddress = deployAtTransaction.getATAccount().getAddress();

			for (int i = 0; i < 10; ++i)
				BlockUtils.mintBlock(repository);
			int blockchainHeight = repository.getBlockRepository().getBlockchainHeight();

			Integer testHeight = blockchainHeight;

			ATData atData = repository.getATRepository().fromATAddress(atAddress);

			byte[] codeHash = atData.getCodeHash();
			Boolean isFinished = Boolean.FALSE;
			Integer dataByteOffset = MachineState.HEADER_LENGTH + 0;
			Long expectedValue = 0L;
			Integer minimumFinalHeight = null;
			Integer limit = null;
			Integer offset = null;
			Boolean reverse = null;

			List<ATStateData> atStates = repository.getATRepository().getMatchingFinalATStates(
					codeHash,
					null,
					null,
					isFinished,
					dataByteOffset,
					expectedValue,
					minimumFinalHeight,
					limit, offset, reverse);

			assertEquals(false, atStates.isEmpty());
			assertEquals(1, atStates.size());

			ATStateData atStateData = atStates.get(0);
			assertEquals(testHeight, atStateData.getHeight());
			assertNotNull(atStateData.getStateData());
		}
	}

	@Test
	public void testGetBlockATStatesAtHeightWithData() throws DataException {
		byte[] creationBytes = AtUtils.buildSimpleAT();

		try (final Repository repository = RepositoryManager.getRepository()) {
			PrivateKeyAccount deployer = Common.getTestAccount(repository, "alice");

			long fundingAmount = 1_00000000L;
			AtUtils.doDeployAT(repository, deployer, creationBytes, fundingAmount);

			for (int i = 0; i < 10; ++i)
				BlockUtils.mintBlock(repository);

			Integer testHeight = 8;
			List<ATStateData> atStates = repository.getATRepository().getBlockATStatesAtHeight(testHeight);

			assertEquals(false, atStates.isEmpty());
			assertEquals(1, atStates.size());

			ATStateData atStateData = atStates.get(0);
			assertEquals(testHeight, atStateData.getHeight());
			assertNull(atStateData.getStateData());
		}
	}

}
