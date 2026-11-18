package org.qortal.repository.hsqldb;

import org.junit.Before;
import org.junit.Test;
import org.qortal.arbitrary.misc.Service;
import org.qortal.data.transaction.ArbitraryTransactionData;
import org.qortal.data.transaction.BaseTransactionData;
import org.qortal.repository.Repository;
import org.qortal.repository.RepositoryManager;
import org.qortal.test.common.Common;
import org.qortal.transaction.Transaction.ApprovalStatus;

import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class ArbitraryLatestTransactionsTests extends Common {

    @Before
    public void beforeTest() throws Exception {
        Common.useDefaultSettings();
    }

    @Test
    public void limitsNewestNamedTransactionsBeforeHydration() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            save(repository, 1, "Alpha", 100);
            ArbitraryTransactionData newest = save(repository, 2, "Beta", 300);
            save(repository, 3, null, 400);
            ArbitraryTransactionData second = save(repository, 4, "Alpha", 200);
            List<ArbitraryTransactionData> results = repository.getArbitraryRepository().getLatestArbitraryTransactions(2);
            assertEquals(2, results.size());
            assertArrayEquals(newest.getSignature(), results.get(0).getSignature());
            assertArrayEquals(second.getSignature(), results.get(1).getSignature());
            assertEquals(Service.DOCUMENT, results.get(0).getService());
            assertEquals("identifier", results.get(0).getIdentifier());
            assertArrayEquals(new byte[]{1}, results.get(0).getData());
            assertEquals(3, repository.getArbitraryRepository().getLatestArbitraryTransactions(20).size());
            assertEquals(3, repository.getArbitraryRepository().getLatestArbitraryTransactions().size());
            // Core enables MySQL syntax, where LIMIT 0 returns no rows.
            assertTrue(repository.getArbitraryRepository().getLatestArbitraryTransactions(0).isEmpty());
        }
    }

    @Test
    public void emptyNamedSetAndNewWritesAreHandled() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            save(repository, 1, null, 100);
            assertTrue(repository.getArbitraryRepository().getLatestArbitraryTransactions(100).isEmpty());
            ArbitraryTransactionData named = save(repository, 2, "Alpha", 200);
            assertArrayEquals(named.getSignature(), repository.getArbitraryRepository()
                    .getLatestArbitraryTransactions(1).get(0).getSignature());
            ArbitraryTransactionData newer = save(repository, 3, "Beta", 300);
            assertArrayEquals(newer.getSignature(), repository.getArbitraryRepository()
                    .getLatestArbitraryTransactions(1).get(0).getSignature());
        }
    }

    @Test
    public void limitedQueryUsesTimestampIndexRatherThanScanningNamedRows() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            assertTimestampPlan(repository);
        }
    }

    @Test
    public void upgradesVersion54WithoutChangingTransactionsAndCanRunAgain() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            ArbitraryTransactionData original = save(repository, 1, "Alpha", 100);
            try (Statement statement = repository.getConnection().createStatement()) {
                statement.execute("DROP INDEX ArbitraryCreatedIndex");
                statement.execute("UPDATE DatabaseInfo SET version = 54");
            }
            repository.saveChanges();
            HSQLDBDatabaseUpdates.updateDatabase(repository.getConnection());
            assertEquals(55, HSQLDBDatabaseUpdates.fetchDatabaseVersion(repository.getConnection()));
            HSQLDBDatabaseUpdates.updateDatabase(repository.getConnection());
            assertTimestampPlan(repository);
            assertArrayEquals(original.getSignature(), repository.getArbitraryRepository()
                    .getLatestArbitraryTransactions(100).get(0).getSignature());
        }
    }

    @Test
    public void migrationAcceptsAnAlreadyPresentTimestampIndex() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            try (Statement statement = repository.getConnection().createStatement()) {
                statement.execute("UPDATE DatabaseInfo SET version = 54");
            }
            repository.saveChanges();
            HSQLDBDatabaseUpdates.updateDatabase(repository.getConnection());
            assertTimestampPlan(repository);
        }
    }

    private static void assertTimestampPlan(Repository repository) throws Exception {
        StringBuilder plan = new StringBuilder();
        try (Statement statement = repository.getConnection().createStatement()) {
            assertTrue(statement.execute("EXPLAIN PLAN FOR "
                    + HSQLDBArbitraryRepository.latestTransactionSignaturesSql(100)));
            try (ResultSet results = statement.getResultSet()) {
                while (results.next()) plan.append(results.getString(1)).append('\n');
            }
        }
        assertTrue(plan.toString(), plan.toString().contains("index=ARBITRARYCREATEDINDEX"));
        assertTrue(plan.toString(), plan.toString().contains("uses index"));
    }

    private static ArbitraryTransactionData save(Repository repository, int id, String name, long timestamp)
            throws Exception {
        byte[] signature = new byte[64];
        ByteBuffer.wrap(signature).putInt(id);
        BaseTransactionData base = new BaseTransactionData(timestamp, 0, new byte[64], new byte[32], 0L,
                ApprovalStatus.NOT_REQUIRED, null, null, signature);
        ArbitraryTransactionData transaction = new ArbitraryTransactionData(base, 5, Service.DOCUMENT.value,
                0, 1, name, "identifier", ArbitraryTransactionData.Method.PUT, null,
                ArbitraryTransactionData.Compression.NONE, new byte[]{1},
                ArbitraryTransactionData.DataType.RAW_DATA, null, Collections.emptyList());
        repository.getTransactionRepository().save(transaction);
        repository.saveChanges();
        return transaction;
    }
}
