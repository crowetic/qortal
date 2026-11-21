package org.qortal.repository.hsqldb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.qortal.data.transaction.ArbitraryTransactionData;
import org.qortal.repository.ArbitraryRepository;
import org.qortal.repository.DataException;
import org.qortal.settings.Settings;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.*;

import static org.junit.Assert.*;

/** Exercises the real HSQLDB projection and query bounds without a running Core. */
public class ArbitraryTransactionBatchTests {
    private Field settingsInstance;
    private Object oldSettings;
    private CountingRepository repository;
    private ArbitraryRepository arbitrary;

    private static final class CountingRepository extends HSQLDBRepository {
        final List<Integer> parameterCounts = new ArrayList<>();
        CountingRepository(Connection connection) throws DataException { super(connection); }
        @Override
        public ResultSet checkedExecute(String sql, Object... parameters) throws SQLException {
            parameterCounts.add(parameters.length);
            return super.checkedExecute(sql, parameters);
        }
    }

    @Before
    public void setUp() throws Exception {
        settingsInstance = Settings.class.getDeclaredField("instance");
        settingsInstance.setAccessible(true);
        oldSettings = settingsInstance.get(null);
        Constructor<Settings> constructor = Settings.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        settingsInstance.set(null, constructor.newInstance());
        Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:arbitrary-batch-" + UUID.randomUUID(), "SA", "");
        try (Statement statement = connection.createStatement()) {
            statement.execute("SET DATABASE SQL SYNTAX MYS TRUE");
            statement.execute("CREATE TABLE Transactions (signature VARBINARY(64) PRIMARY KEY, reference VARBINARY(64), type TINYINT, creator VARBINARY(32), created_when BIGINT, fee BIGINT, tx_group_id INT, block_height INT, approval_status TINYINT, approval_height INT)");
            statement.execute("CREATE TABLE ArbitraryTransactions (signature VARBINARY(64) PRIMARY KEY, version TINYINT, nonce INT, service SMALLINT, size INT, is_data_raw BOOLEAN, data VARBINARY(4000), metadata_hash VARBINARY(32), name VARCHAR(400), identifier VARCHAR(64), update_method INT, secret VARBINARY(32), compression INT, created_when BIGINT)");
            statement.execute("CREATE INDEX ArbitraryNameCreatedIndex ON ArbitraryTransactions(name, created_when DESC)");
        }
        connection.setAutoCommit(false);
        repository = new CountingRepository(connection);
        arbitrary = repository.getArbitraryRepository();
    }

    @After
    public void tearDown() throws Exception {
        try {
            if (repository != null) {
                try (Statement statement = repository.getConnection().createStatement()) { statement.execute("SHUTDOWN"); }
                repository.getConnection().close();
            }
        } finally {
            settingsInstance.set(null, oldSettings);
        }
    }

    private static byte[] signature(int id) {
        return ByteBuffer.allocate(64).putInt(id).array();
    }

    private void save(int id, long timestamp, String name, int method, int service, boolean raw) throws Exception {
        try (PreparedStatement tx = repository.getConnection().prepareStatement("INSERT INTO Transactions VALUES(?,?,?,?,?,?,?,?,?,?)");
             PreparedStatement arb = repository.getConnection().prepareStatement("INSERT INTO ArbitraryTransactions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
            tx.setBytes(1, signature(id)); tx.setBytes(2, signature(91)); tx.setInt(3, 10);
            tx.setBytes(4, new byte[32]); tx.setLong(5, timestamp); tx.setNull(6, Types.BIGINT);
            tx.setInt(7, 7); tx.setNull(8, Types.INTEGER); tx.setInt(9, 0); tx.setNull(10, Types.INTEGER);
            tx.executeUpdate();
            arb.setBytes(1, signature(id)); arb.setInt(2, 5); arb.setInt(3, 123); arb.setInt(4, service);
            arb.setInt(5, 4096); arb.setBoolean(6, raw); arb.setBytes(7, new byte[] {4, 5, 6});
            arb.setBytes(8, new byte[32]); arb.setString(9, name); arb.setString(10, "layer");
            arb.setInt(11, method); arb.setBytes(12, new byte[32]); arb.setInt(13, 1); arb.setLong(14, timestamp);
            arb.executeUpdate();
        }
    }

    @Test
    public void snapshotIncludesAllNamedHistoryWithoutJoiningTransactions() throws Exception {
        save(1, 100, "same", 0, 100, false);
        save(2, 200, "same", 1, 100, false);
        save(3, 300, "other", 0, 999, true);
        save(4, 400, null, 0, 100, false);
        try (Statement statement = repository.getConnection().createStatement()) { statement.execute("DROP TABLE Transactions"); }
        List<byte[]> signatures = arbitrary.getArbitraryTransactionSignatures();
        assertEquals(3, signatures.size());
        assertArrayEquals(signature(3), signatures.get(0));
        assertArrayEquals(signature(2), signatures.get(1));
        assertArrayEquals(signature(1), signatures.get(2));
        assertEquals(Collections.singletonList(0), repository.parameterCounts);
    }

    @Test
    public void hydrationPreservesRequestedOrderTiesAndDuplicatesAndSkipsMissing() throws Exception {
        save(1, 100, "same", 0, 100, false);
        save(2, 100, "same", 1, 100, false);
        save(3, 200, "other", 0, 100, false);
        try (Statement statement = repository.getConnection().createStatement()) {
            statement.execute("DELETE FROM Transactions WHERE signature = X'" + hex(signature(3)) + "'");
        }
        List<ArbitraryTransactionData> transactions = arbitrary.getArbitraryTransactionsBySignatures(
                Arrays.asList(signature(2), signature(3), signature(99), signature(1), signature(2), null, new byte[0]));
        assertEquals(3, transactions.size());
        assertArrayEquals(signature(2), transactions.get(0).getSignature());
        assertArrayEquals(signature(1), transactions.get(1).getSignature());
        assertArrayEquals(signature(2), transactions.get(2).getSignature());
        assertEquals(Collections.singletonList(4), repository.parameterCounts);
    }

    private static String hex(byte[] bytes) {
        StringBuilder result = new StringBuilder();
        for (byte value : bytes) result.append(String.format("%02x", value));
        return result.toString();
    }

    @Test
    public void hydrationRetainsCleanupFieldsAndNullableBaseFields() throws Exception {
        save(1, 123456, "test", 1, 100, false);
        ArbitraryTransactionData data = arbitrary.getArbitraryTransactionsBySignatures(Collections.singletonList(signature(1))).get(0);
        assertEquals(123456, data.getTimestamp());
        assertEquals(7, data.getTxGroupId());
        assertNull(data.getFee()); assertNull(data.getBlockHeight()); assertNull(data.getApprovalHeight());
        assertArrayEquals(signature(91), data.getReference());
        assertArrayEquals(new byte[32], data.getCreatorPublicKey());
        assertEquals(5, data.getVersion()); assertEquals(123, data.getNonce()); assertEquals(4096, data.getSize());
        assertEquals("test", data.getName()); assertEquals("layer", data.getIdentifier());
        assertEquals(ArbitraryTransactionData.Method.PATCH, data.getMethod());
        assertEquals(ArbitraryTransactionData.Compression.ZIP, data.getCompression());
        assertEquals(ArbitraryTransactionData.DataType.DATA_HASH, data.getDataType());
        assertArrayEquals(new byte[] {4, 5, 6}, data.getData());
        assertArrayEquals(new byte[32], data.getMetadataHash()); assertArrayEquals(new byte[32], data.getSecret());
        assertNull(data.getPayments());
    }

    @Test
    public void hydrationSplitsLargeRequestsIntoBoundedQueries() throws Exception {
        List<byte[]> signatures = new ArrayList<>();
        for (int id = 1001; id > 0; id--) {
            save(id, id, "test", 0, 100, id % 2 == 0);
            signatures.add(signature(id));
        }
        List<ArbitraryTransactionData> transactions = arbitrary.getArbitraryTransactionsBySignatures(signatures);
        assertEquals(1001, transactions.size());
        for (int index = 0; index < signatures.size(); index++)
            assertArrayEquals(signatures.get(index), transactions.get(index).getSignature());
        assertEquals(Arrays.asList(500, 500, 1), repository.parameterCounts);
    }

    @Test
    public void emptyRequestsDoNotQueryAndEmptySnapshotsAreSupported() throws Exception {
        assertTrue(arbitrary.getArbitraryTransactionsBySignatures(null).isEmpty());
        assertTrue(arbitrary.getArbitraryTransactionsBySignatures(Collections.emptyList()).isEmpty());
        assertTrue(arbitrary.getArbitraryTransactionsBySignatures(Arrays.asList(null, new byte[0])).isEmpty());
        assertTrue(repository.parameterCounts.isEmpty());
        assertTrue(arbitrary.getArbitraryTransactionSignatures().isEmpty());
    }

    @Test
    public void queryFailuresAreReportedInsteadOfReturningAnEmptySweep() throws Exception {
        try (Statement statement = repository.getConnection().createStatement()) { statement.execute("DROP TABLE ArbitraryTransactions"); }
        try {
            arbitrary.getArbitraryTransactionSignatures();
            fail("Expected a repository failure");
        } catch (DataException expected) {
            // Expected: callers must be able to retry failed queries.
        }
        try {
            arbitrary.getArbitraryTransactionsBySignatures(Collections.singletonList(signature(1)));
            fail("Expected a repository failure");
        } catch (DataException expected) {
            // Expected: callers must be able to retry failed queries.
        }
    }

    @Test
    public void existingLatestQueriesKeepTheirProjectionAndOrdering() throws Exception {
        save(1, 100, "same", 0, 100, false);
        save(2, 200, "same", 1, 100, true);
        save(3, 300, null, 0, 100, false);
        List<ArbitraryTransactionData> all = arbitrary.getLatestArbitraryTransactions();
        assertEquals(2, all.size());
        assertArrayEquals(signature(2), all.get(0).getSignature());
        assertEquals(ArbitraryTransactionData.DataType.RAW_DATA, all.get(0).getDataType());
        assertEquals(ArbitraryTransactionData.Method.PATCH, all.get(0).getMethod());
        List<ArbitraryTransactionData> limited = arbitrary.getLatestArbitraryTransactions(1);
        assertEquals(1, limited.size());
        assertArrayEquals(signature(2), limited.get(0).getSignature());
    }
}
