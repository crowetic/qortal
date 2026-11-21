package org.qortal.test.at;

import org.junit.Before;
import org.junit.Test;
import org.qortal.at.AT;
import org.qortal.at.ATExecutionContext;
import org.qortal.at.ATBlockResult;
import org.qortal.controller.Controller;
import org.qortal.data.at.ATData;
import org.qortal.data.at.ATStateData;
import org.qortal.repository.*;
import org.qortal.test.common.*;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.sql.ResultSet;

import static org.junit.Assert.*;

public class ATStoreTransactionTests extends Common {
    @Before public void setup() throws DataException { Common.useDefaultSettings(); }

    @Test public void commitNotificationsFollowNestedSavepoints() throws DataException {
        List<Integer> delivered = new ArrayList<>();
        try (Repository r = RepositoryManager.getRepository()) {
            r.afterCommit(() -> delivered.add(1));
            r.setSavepoint();
            r.afterCommit(() -> delivered.add(2));
            r.setSavepoint();
            r.afterCommit(() -> delivered.add(3));
            r.rollbackToSavepoint();
            assertTrue(delivered.isEmpty());
            r.saveChanges();
            assertEquals(Arrays.asList(1,2), delivered);
            r.afterCommit(() -> delivered.add(4));
            r.discardChanges();
            r.saveChanges();
            assertEquals(Arrays.asList(1,2), delivered);
        }
    }

    @Test public void recipientWakeStateRollsBackWithConfirmation() throws Exception {
        try (Repository r = RepositoryManager.getRepository()) {
            org.qortal.account.PrivateKeyAccount alice = Common.getTestAccount(r,"alice");
            String address = AtUtils.doDeployAT(r,alice,AtUtils.buildSimpleAT(),1_00000000L).getATAccount().getAddress();
            int height = r.getBlockRepository().getBlockchainHeight();
            ATData head = r.getATRepository().fromATAddress(address);
            head.setIsSleeping(true);
            head.setSleepUntilHeight(0);
            head.setSleepUntilMessageTimestamp(org.ciyam.at.Timestamp.toLong(height,0));
            r.getATRepository().save(head);
            org.qortal.data.transaction.BaseTransactionData base = new org.qortal.data.transaction.BaseTransactionData(
                    System.currentTimeMillis(),0,new byte[64],alice.getPublicKey(),1L,new byte[64]);
            org.qortal.data.transaction.PaymentTransactionData payment = new org.qortal.data.transaction.PaymentTransactionData(base,address,1L);
            payment.setApprovalStatus(org.qortal.transaction.Transaction.ApprovalStatus.NOT_REQUIRED);
            r.getTransactionRepository().save(payment);
            assertTrue(r.getATRepository().getATsForExecution(height+1).isEmpty());
            r.setSavepoint();
            r.getTransactionRepository().updateBlockPositions(Collections.singletonList(payment),height+1);
            assertEquals(1,r.getATRepository().getATsForExecution(height+2).size());
            assertNotNull(r.getATRepository().findNextTransaction(address,height,0));
            r.setSavepoint();
            r.getTransactionRepository().updateBlockHeight(payment.getSignature(),null);
            assertTrue(r.getATRepository().getATsForExecution(height+2).isEmpty());
            r.rollbackToSavepoint();
            assertEquals(1,r.getATRepository().getATsForExecution(height+2).size());
            r.rollbackToSavepoint();
            assertNull(r.getATRepository().findNextTransaction(address,height,0));
            assertTrue(r.getATRepository().getATsForExecution(height+2).isEmpty());
            r.discardChanges();
        }
    }

    @Test public void frozenFastPathMatchesInterpreterAndWakesOnFunding() throws Exception {
        try (Repository r = RepositoryManager.getRepository()) {
            String address = AtUtils.doDeployAT(r,Common.getTestAccount(r,"alice"),AtUtils.buildSimpleAT(),1_00000000L).getATAccount().getAddress();
            r.getAccountRepository().setAssetBalances(Collections.singletonList(new org.qortal.data.account.AccountBalanceData(address,0,0)));
            int height = r.getBlockRepository().getBlockchainHeight()+1;
            long timestamp = System.currentTimeMillis();
            ATData data = r.getATRepository().fromATAddress(address);
            AT freeze = new AT(r,data);
            freeze.run(height,timestamp);
            assertTrue(freeze.prepareUpdate().getIsFrozen());
            saveTestBlockStates(r, Collections.singletonList(freeze.prepareUpdate()),Collections.singletonList(freeze.getATStateData()));
            for (long balance : new long[]{0,1_00000000L}) {
                r.getAccountRepository().setAssetBalances(Collections.singletonList(new org.qortal.data.account.AccountBalanceData(address,0,balance)));
                data = r.getATRepository().fromATAddress(address);
                AT at = new AT(r,data);
                at.run(height+1,timestamp,new ATExecutionContext(r,Collections.singletonList(data)));
                assertNotNull(at.getATStateData());
                if (balance == 0) {
                    assertEquals(Long.valueOf(0),at.getATStateData().getFees());
                    assertTrue(at.getATStateData() instanceof org.qortal.data.at.ATStateReference);
                    assertArrayEquals(freeze.getATStateData().getStateData(),r.getATRepository().getStatePayload(at.getATStateData().getStateHash()));
                }
            }
            r.discardChanges();
        }
    }

    @Test public void reusedPayloadRequiresExactParentAndSurvivesRollback() throws Exception {
        try (Repository r = RepositoryManager.getRepository()) {
            String address = AtUtils.doDeployAT(r,Common.getTestAccount(r,"alice"),AtUtils.buildSimpleAT(),1_00000000L).getATAccount().getAddress();
            org.qortal.data.at.ATExecutionState parent = r.getATRepository().getExecutionStates(Collections.singletonList(address)).get(0);
            ATStateData receipt = new org.qortal.data.at.ATStateReference(parent,parent.getHeight()+1,0L,null);
            r.setSavepoint();
            saveTestBlockStates(r, Collections.emptyList(),Collections.singletonList(receipt));
            assertArrayEquals(parent.getHash(),r.getATRepository().getLatestATState(address).getStateHash());
            assertNotNull(r.getATRepository().getLatestATState(address).getStateData());
            r.rollbackToSavepoint();
            assertEquals(Integer.valueOf(parent.getHeight()),r.getATRepository().getLatestATState(address).getHeight());
            saveTestBlockStates(r, Collections.emptyList(),Collections.singletonList(receipt));
            r.setSavepoint();
            try {
                saveTestBlockStates(r, Collections.emptyList(),Collections.singletonList(new org.qortal.data.at.ATStateReference(parent,parent.getHeight()+2,0L,null)));
                fail("stale parent must be rejected");
            } catch (DataException expected) { r.rollbackToSavepoint(); }
            assertEquals(Integer.valueOf(parent.getHeight()+1),r.getATRepository().getLatestATState(address).getHeight());
            r.discardChanges();
        }
    }

    @Test public void batchedReferencesPreserveExistingPublicKeys() throws Exception {
        try (Repository r = RepositoryManager.getRepository()) {
            org.qortal.account.PrivateKeyAccount alice = Common.getTestAccount(r,"alice");
            org.qortal.data.account.AccountData existing = r.getAccountRepository().getAccount(alice.getAddress());
            org.qortal.data.account.AccountData update = new org.qortal.data.account.AccountData(alice.getAddress());
            byte[] reference = new byte[64]; reference[0]=7;
            update.setReference(reference);
            r.getAccountRepository().setLastReferences(Collections.singletonList(update));
            assertArrayEquals(existing.getPublicKey(),r.getAccountRepository().getAccount(alice.getAddress()).getPublicKey());
            assertArrayEquals(reference,r.getAccountRepository().getLastReference(alice.getAddress()));
            r.discardChanges();
        }
    }

    @Test public void headAndPayloadRollback() throws Exception {
        try (Repository r = RepositoryManager.getRepository()) {
            String address = AtUtils.doDeployAT(r, Common.getTestAccount(r,"alice"), AtUtils.buildSimpleAT(), 1_00000000L).getATAccount().getAddress();
            ATStateData original = r.getATRepository().getLatestATState(address);
            ATData data = r.getATRepository().fromATAddress(address);
            int height = r.getBlockRepository().getBlockchainHeight()+1;
            long timestamp = System.currentTimeMillis();
            AT at = new AT(r, data);
            at.run(height, timestamp, new ATExecutionContext(r, Collections.singletonList(data)));
            ATStateData next = at.getATStateData();
            assertNotNull(next);
            r.setSavepoint();
            saveTestBlockStates(r, Collections.singletonList(at.prepareUpdate()),Collections.singletonList(next));
            assertEquals(Integer.valueOf(height),r.getATRepository().getLatestATState(address).getHeight());
            r.rollbackToSavepoint();
            assertEquals(original.getHeight(),r.getATRepository().getLatestATState(address).getHeight());
            assertArrayEquals(original.getStateData(),r.getATRepository().getLatestATState(address).getStateData());
            // A cached immutable payload from the rolled-back insertion must not imply DB existence.
            saveTestBlockStates(r, Collections.singletonList(at.prepareUpdate()),Collections.singletonList(next));
            assertEquals(1, ATBlockResult.deserialize(r.getATRepository().getBlockResult(height)).size());
			try (ResultSet rows = ((org.qortal.repository.hsqldb.HSQLDBRepository) r).checkedExecute(
					"SELECT payload_bytes FROM ATBlockPayloads WHERE block_height=?", height)) {
				assertNotNull(rows);
				assertNotNull(rows.getBytes(1));
			}
            r.saveChanges();
            ReentrantLock lock = Controller.getInstance().getBlockchainLock();
            lock.lock();
            try {
                r.saveChanges();
            } finally { lock.unlock(); }
            assertArrayEquals(next.getStateData(),r.getATRepository().getLatestATState(address).getStateData());
            r.discardChanges();
        }
    }

    @Test public void blockResultWriterProducesOrphanableVersionedResult() throws Exception {
        try (Repository r = RepositoryManager.getRepository()) {
            String address = AtUtils.doDeployAT(r, Common.getTestAccount(r, "alice"), AtUtils.buildSimpleAT(), 1_00000000L)
                    .getATAccount().getAddress();
            ATData parentHead = r.getATRepository().fromATAddress(address);
            ATStateData parentState = r.getATRepository().getLatestATState(address);
            int height = r.getBlockRepository().getBlockchainHeight() + 1;

            AT at = new AT(r, parentHead);
            at.run(height, System.currentTimeMillis(), new ATExecutionContext(r, Collections.singletonList(parentHead)));
            ATStateData next = at.getATStateData();
            saveTestBlockStates(r, Collections.singletonList(at.prepareUpdate()), Collections.singletonList(next));

            ATBlockResult.Entry entry = ATBlockResult.deserialize(r.getATRepository().getBlockResult(height)).get(0);
            assertEquals(parentState.getHeight().intValue(), entry.prevHeight);
            assertArrayEquals(parentState.getStateHash(), entry.prevStateHash);

            r.getATRepository().deleteBlockResult(height);
            ATStateData restored = r.getATRepository().getLatestATState(address);
            assertEquals(parentState.getHeight(), restored.getHeight());
            assertArrayEquals(parentState.getStateHash(), restored.getStateHash());
            r.discardChanges();
        }
    }

	@Test public void blockResultCapturesParentMetadataAndSurvivesOrphan() throws Exception {
		try (Repository r = RepositoryManager.getRepository()) {
			String address = AtUtils.doDeployAT(r, Common.getTestAccount(r, "alice"), AtUtils.buildSimpleAT(), 1_00000000L)
					.getATAccount().getAddress();
			ATData parent = r.getATRepository().fromATAddress(address);
			ATStateData parentState = r.getATRepository().getLatestATState(address);
			Object[] parentCurrentState = currentStateRow(r, address);

			org.qortal.block.Block block = BlockUtils.mintBlock(r);
			byte[] blob = r.getATRepository().getBlockResult(block.getBlockData().getHeight());
			assertNotNull(blob);
			ATBlockResult.Entry receipt = ATBlockResult.deserialize(blob).stream()
					.filter(entry -> address.equals(entry.address)).findFirst().orElseThrow(AssertionError::new);
			assertEquals(parent.getIsSleeping(), receipt.prevIsSleeping);
			assertEquals(parent.getSleepUntilHeight(), receipt.prevSleepUntilHeight);
			assertEquals(parent.getIsFinished(), receipt.prevIsFinished);
			assertEquals(parent.getHadFatalError(), receipt.prevHadFatalError);
			assertEquals(parent.getIsFrozen(), receipt.prevIsFrozen);
			assertEquals(parent.getFrozenBalance(), receipt.prevFrozenBalance);
			assertEquals(parent.getSleepUntilMessageTimestamp(), receipt.prevSleepUntilMessageTimestamp);

			ReentrantLock lock = Controller.getInstance().getBlockchainLock();
			lock.lock();
			try {
				r.saveChanges();
			} finally {
				lock.unlock();
			}

			BlockUtils.orphanLastBlock(r);
			ATData restored = r.getATRepository().fromATAddress(address);
			assertEquals(parent.getIsSleeping(), restored.getIsSleeping());
			assertEquals(parent.getSleepUntilHeight(), restored.getSleepUntilHeight());
			assertEquals(parent.getIsFinished(), restored.getIsFinished());
			assertEquals(parent.getHadFatalError(), restored.getHadFatalError());
			assertEquals(parent.getIsFrozen(), restored.getIsFrozen());
			assertEquals(parent.getFrozenBalance(), restored.getFrozenBalance());
			assertEquals(parent.getSleepUntilMessageTimestamp(), restored.getSleepUntilMessageTimestamp());
			assertArrayEquals(parentState.getStateHash(), r.getATRepository().getLatestATState(address).getStateHash());
			assertCurrentStateRowEquals(parentCurrentState, currentStateRow(r, address));
		}
	}

	private static void saveTestBlockStates(Repository repository, List<ATData> heads,
			List<ATStateData> states) throws DataException {
		if (states.isEmpty())
			return;
		int height = states.get(0).getHeight();
		List<ATBlockResult.Entry> entries = new ArrayList<>(states.size());
		for (ATStateData state : states) {
			ATData parentHead = repository.getATRepository().fromATAddress(state.getATAddress());
			ATStateData parentState = repository.getATRepository().getLatestATState(state.getATAddress());
			if (state.isInitial()) {
				entries.add(new ATBlockResult.Entry(state.getATAddress(), state.getStateHash(), state.getFees(),
						true, state.getSleepUntilMessageTimestamp(), -1, null,
						false, null, false, false, false, null, null));
			} else {
				org.qortal.data.at.ATExecutionState parent = new org.qortal.data.at.ATExecutionState(
						state.getATAddress(), parentState.getHeight(), parentState.getStateHash(), 0L);
				entries.add(new ATBlockResult.Entry(state, new ATBlockResult.ParentMetadata(parentHead), parent));
			}
		}
		repository.getATRepository().saveBlockResult(height, ATBlockResult.serialize(entries), heads, states);
	}

	private static Object[] currentStateRow(Repository repository, String address) throws Exception {
		try (ResultSet rows = ((org.qortal.repository.hsqldb.HSQLDBRepository) repository).checkedExecute(
				"SELECT latest_state_height,payload_hash,is_sleeping,sleep_until_height,is_finished,had_fatal_error,"
						+ "is_frozen,frozen_balance,sleep_until_message_timestamp,cursor_height,cursor_sequence,schedule_class,"
						+ "message_pending,previous_state_height,previous_payload_hash,rollback_state_height,rollback_payload_hash "
						+ "FROM ATCurrentState WHERE AT_address=?",
				address)) {
			assertNotNull(rows);
			Object[] values = new Object[17];
			for (int index = 0; index < values.length; ++index)
				values[index] = index == 1 || index == 14 || index == 16 ? rows.getBytes(index + 1) : rows.getObject(index + 1);
			return values;
		}
	}

	private static void assertCurrentStateRowEquals(Object[] expected, Object[] actual) {
		assertEquals(expected.length, actual.length);
		for (int index = 0; index < expected.length; ++index) {
			if (expected[index] instanceof byte[] || actual[index] instanceof byte[])
				assertArrayEquals("column " + (index + 1), (byte[]) expected[index], (byte[]) actual[index]);
			else
				assertEquals("column " + (index + 1), expected[index], actual[index]);
		}
	}
}
