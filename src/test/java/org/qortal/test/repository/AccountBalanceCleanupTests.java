package org.qortal.test.repository;

import org.junit.Before;
import org.junit.Test;
import org.qortal.data.account.AccountBalanceData;
import org.qortal.repository.Repository;
import org.qortal.repository.RepositoryManager;
import org.qortal.repository.hsqldb.HSQLDBDatabaseUpdates;
import org.qortal.test.common.Common;

import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.*;

public class AccountBalanceCleanupTests extends Common {

    @Before
    public void beforeTest() throws Exception {
        Common.useDefaultSettings();
    }

    @Test
    public void testIndexedTidyPreservesPositiveBalancesAndCanBeRolledBack() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            String alice = Common.getTestAccount(repository, "alice").getAddress();
            String bob = Common.getTestAccount(repository, "bob").getAddress();
            saveBalance(repository, alice, 999L, 0L);
            saveBalance(repository, alice, 1000L, 0L);
            saveBalance(repository, bob, 999L, 17L);
            saveBalance(repository, bob, 1000L, 5L);
            repository.saveChanges();
            assertIndexedCleanup(repository);

            repository.getAccountRepository().tidy();
            assertNull(repository.getAccountRepository().getBalance(alice, 999L));
            assertNull(repository.getAccountRepository().getBalance(alice, 1000L));
            assertEquals(17L, repository.getAccountRepository().getBalance(bob, 999L).getBalance());
            assertEquals(5L, repository.getAccountRepository().getBalance(bob, 1000L).getBalance());

            repository.discardChanges();
            assertEquals(0L, repository.getAccountRepository().getBalance(alice, 999L).getBalance());
            assertEquals(0L, repository.getAccountRepository().getBalance(alice, 1000L).getBalance());
            repository.getAccountRepository().tidy();
            repository.saveChanges();
            repository.getAccountRepository().tidy();
            repository.saveChanges();
            assertNull(repository.getAccountRepository().getBalance(alice, 999L));
            assertEquals(17L, repository.getAccountRepository().getBalance(bob, 999L).getBalance());
        }
    }

    @Test
    public void testVersion53UpgradePreservesRowsAndHandlesInterruptedIndexCreation() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            String alice = Common.getTestAccount(repository, "alice").getAddress();
            saveBalance(repository, alice, 999L, 17L);
            repository.saveChanges();

            // Test both an ordinary upgrade and a retry after DDL committed but version update did not.
            for (boolean indexAlreadyExists : new boolean[]{false, true}) {
                try (Statement statement = repository.getConnection().createStatement()) {
                    if (!indexAlreadyExists) statement.execute("DROP INDEX AccountBalancesBalanceIndex");
                    statement.executeUpdate("UPDATE DatabaseInfo SET version = 53");
                }
                repository.saveChanges();
                HSQLDBDatabaseUpdates.updateDatabase(repository.getConnection());
                assertEquals(54, HSQLDBDatabaseUpdates.fetchDatabaseVersion(repository.getConnection()));
                assertEquals(17L, repository.getAccountRepository().getBalance(alice, 999L).getBalance());
                assertIndexedCleanup(repository);
            }
        }
    }

    @Test
    public void testBalanceIndexAndDataPersistAfterDiskRepositoryReopen() throws Exception {
        Common.useSettingsAndDb(Common.testSettingsFilename, false);
        String alice;
        try (Repository repository = RepositoryManager.getRepository()) {
            alice = Common.getTestAccount(repository, "alice").getAddress();
            saveBalance(repository, alice, 999L, 17L);
            repository.saveChanges();
            assertIndexedCleanup(repository);
        }
        RepositoryManager.closeRepositoryFactory();
        Common.setRepository(false);
        try (Repository repository = RepositoryManager.getRepository()) {
            assertEquals(17L, repository.getAccountRepository().getBalance(alice, 999L).getBalance());
            assertIndexedCleanup(repository);
        }
    }

    private static void assertIndexedCleanup(Repository repository) throws Exception {
        StringBuilder plan = new StringBuilder();
        try (Statement statement = repository.getConnection().createStatement()) {
            assertTrue(statement.execute("EXPLAIN PLAN FOR DELETE FROM AccountBalances WHERE balance = 0"));
            try (ResultSet resultSet = statement.getResultSet()) {
                while (resultSet.next()) plan.append(resultSet.getString(1));
            }
        }
        assertTrue("Cleanup must use the balance index: " + plan,
                plan.toString().contains("index=ACCOUNTBALANCESBALANCEINDEX"));
        assertTrue(plan.toString().contains("access=INDEX PRED"));
    }

    private static void saveBalance(Repository repository, String address, long assetId, long balance) throws Exception {
        repository.getAccountRepository().save(new AccountBalanceData(address, assetId, balance));
    }
}
