package org.qortal.repository.hsqldb;

import org.junit.Test;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.qortal.crypto.Crypto;
import org.qortal.at.ATStateBundle;
import org.qortal.repository.ATRepository;
import org.qortal.settings.Settings;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.*;
import java.security.Security;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/** Non-empty legacy fixtures, including restart in the middle of a backfill. */
public class ATStoreMigrationTests {
    private Connection legacy(int states) throws Exception {
        return legacy(states,"jdbc:hsqldb:mem:at-migrate-" + UUID.randomUUID());
    }

    private Connection legacy(int states, String url) throws Exception {
        Connection c = DriverManager.getConnection(url);
        String[] ddl = {
                "SET DATABASE SQL SYNTAX MYS TRUE", "SET FILES LOG FALSE",
                "CREATE TYPE QortalAddress AS VARCHAR(36)", "CREATE TYPE ATCode AS VARBINARY(1024)",
                "CREATE TYPE ATState AS VARBINARY(1024)", "CREATE TYPE ATStateHash AS VARBINARY(32)",
                "CREATE TYPE QortalAmount AS BIGINT", "CREATE TYPE Signature AS VARBINARY(64)", "CREATE TYPE BlockSignature AS VARBINARY(128)",
                "CREATE TABLE DatabaseInfo(version INTEGER, AT_trim_height INTEGER, AT_prune_height INTEGER)",
                "INSERT INTO DatabaseInfo VALUES(53,0,0)",
                "CREATE TABLE Blocks(signature BlockSignature, height INTEGER)",
                "CREATE TABLE Transactions(signature Signature PRIMARY KEY, type INTEGER, block_height INTEGER, block_sequence INTEGER)",
                "CREATE TABLE PaymentTransactions(signature Signature PRIMARY KEY,recipient QortalAddress)",
                "CREATE TABLE MessageTransactions(signature Signature PRIMARY KEY,recipient QortalAddress)",
                "CREATE TABLE ATTransactions(signature Signature PRIMARY KEY,recipient QortalAddress)",
                "CREATE TABLE ATs(AT_address QortalAddress PRIMARY KEY,creator VARBINARY(32),created_when BIGINT,version INTEGER,asset_id BIGINT,"
                        + "code_bytes ATCode,code_hash VARBINARY(32),creation_group_id INTEGER DEFAULT 0,is_sleeping BOOLEAN,sleep_until_height INTEGER,"
                        + "is_finished BOOLEAN,had_fatal_error BOOLEAN,is_frozen BOOLEAN,frozen_balance BIGINT,sleep_until_message_timestamp BIGINT)",
                "CREATE TABLE ATStates(AT_address QortalAddress,height INTEGER,state_hash ATStateHash,fees BIGINT,is_initial BOOLEAN,"
                        + "sleep_until_message_timestamp BIGINT,PRIMARY KEY(AT_address,height),FOREIGN KEY(AT_address) REFERENCES ATs(AT_address) ON DELETE CASCADE)",
                "CREATE TABLE ATStatesData(AT_address QortalAddress,height INTEGER,state_data ATState,PRIMARY KEY(height,AT_address))"
        };
        try (Statement s = c.createStatement()) { for (String sql : ddl) s.execute(sql); }
        c.setAutoCommit(false);
        byte[] code = {1,2,3};
        try (PreparedStatement s = c.prepareStatement("INSERT INTO ATs VALUES('at',?,1,2,0,?,?,0,FALSE,NULL,FALSE,FALSE,FALSE,NULL,NULL)")) {
            s.setBytes(1,new byte[32]); s.setBytes(2,code); s.setBytes(3,Crypto.digest(code)); s.executeUpdate();
        }
        try (PreparedStatement meta = c.prepareStatement("INSERT INTO ATStates VALUES('at',?,?,0,FALSE,NULL)");
             PreparedStatement data = c.prepareStatement("INSERT INTO ATStatesData VALUES('at',?,?)")) {
            for (int height = 1; height <= states; ++height) {
                byte[] bytes = {(byte)(height % 7),42};
                meta.setInt(1,height); meta.setBytes(2,Crypto.digest(bytes)); meta.addBatch();
                data.setInt(1,height); data.setBytes(2,bytes); data.addBatch();
            }
            meta.executeBatch(); data.executeBatch();
        }
        try (Statement s = c.createStatement()) {
            s.execute("INSERT INTO Transactions VALUES(X'01',2,7,NULL)");
            s.execute("INSERT INTO PaymentTransactions VALUES(X'01','at')");
            s.execute("INSERT INTO Transactions VALUES(X'02',18,NULL,NULL)");
            s.execute("INSERT INTO MessageTransactions VALUES(X'02','at')");
            s.execute("INSERT INTO Transactions VALUES(X'03',18,8,0)");
            s.execute("INSERT INTO MessageTransactions VALUES(X'03',NULL)");
			s.execute("INSERT INTO Transactions VALUES(X'04',2,9,0)");
			s.execute("INSERT INTO PaymentTransactions VALUES(X'04','not-an-at')");
        }
        c.commit();
        return c;
    }

    private long count(Connection c, String query) throws SQLException {
        try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(query)) { assertTrue(r.next()); return r.getLong(1); }
    }

    private void assertMigrated(Connection c, int states) throws Exception {
        assertEquals(6,count(c,"SELECT phase FROM ATStoreInfo"));
		assertEquals(5,count(c,"SELECT format_version FROM ATStoreInfo"));
		assertEquals(1,count(c,"SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='ATBLOCKPAYLOADS' AND COLUMN_NAME='PAYLOAD_BYTES'"));
		assertEquals(1,count(c,"SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='ATBLOCKRESULTS' AND COLUMN_NAME='RESULT_BYTES'"));
		assertEquals(0,count(c,"SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='ATBLOCKRESULTS' AND COLUMN_NAME='PAYLOAD_BYTES'"));
		assertEquals(0,count(c,"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='ATSTATEPAYLOADS'"));
		assertEquals(1,count(c,"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='ATRETAINEDSTATEPAYLOADS'"));
		assertEquals(0,count(c,"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='LEGACYATSTATESDATA'"));
		assertEquals(0,count(c,"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='ATSTATESDATA'"));
		assertEquals(states,count(c,"SELECT COUNT(*) FROM ATBlockPayloads WHERE payload_bytes IS NOT NULL"));
        assertEquals(states,count(c,"SELECT latest_state_height FROM ATCurrentState"));
        assertEquals(1,count(c,"SELECT COUNT(*) FROM ATRecipientTransactions"));
        assertEquals(1,count(c,"SELECT COUNT(*) FROM ATRecipientTransactions WHERE block_sequence IS NULL"));
        assertEquals(1,count(c,"SELECT COUNT(*) FROM ATs WHERE is_finished=FALSE"));
        assertEquals(0,count(c,"SELECT COUNT(*) FROM ATStates WHERE payload_hash IS NULL"));
        HSQLDBATStoreMigration.migrate(c); // completed migrations are idempotent
    }

    @Test
    public void directMigrationPreservesEveryPayloadAndNullPosition() throws Exception {
        try (Connection c = legacy(4100)) {
            HSQLDBATStoreMigration.migrate(c);
            assertMigrated(c,4100);
			try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(
					"SELECT payload_bytes FROM ATBlockPayloads WHERE block_height=4099")) {
				assertTrue(r.next());
				ATStateBundle.Entry entry = ATStateBundle.deserialize(r.getBytes(1)).get("at");
				assertNotNull(entry);
				assertArrayEquals(new byte[]{(byte) (4099 % 7), 42}, entry.data);
			}
        }
    }

	@Test
	public void directMigrationCompletesBlockAtChunkBoundary() throws Exception {
		final int heights = 5_462; // Three rows per height crosses the 16,384-row window mid-block.
		try (Connection c = legacy(heights)) {
			try (Statement s = c.createStatement()) {
				s.execute("INSERT INTO ATs SELECT 'at2',creator,created_when,version,asset_id,code_bytes,code_hash,"
						+ "creation_group_id,is_sleeping,sleep_until_height,is_finished,had_fatal_error,is_frozen,"
						+ "frozen_balance,sleep_until_message_timestamp FROM ATs WHERE AT_address='at'");
				s.execute("INSERT INTO ATs SELECT 'at3',creator,created_when,version,asset_id,code_bytes,code_hash,"
						+ "creation_group_id,is_sleeping,sleep_until_height,is_finished,had_fatal_error,is_frozen,"
						+ "frozen_balance,sleep_until_message_timestamp FROM ATs WHERE AT_address='at'");
			}
			try (PreparedStatement meta = c.prepareStatement("INSERT INTO ATStates VALUES(?,?,?,0,FALSE,NULL)");
				 PreparedStatement data = c.prepareStatement("INSERT INTO ATStatesData VALUES(?,?,?)")) {
				for (int height = 1; height <= heights; ++height) {
					for (int suffix = 2; suffix <= 3; ++suffix) {
						String address = "at" + suffix;
						byte[] bytes = {(byte) (height % 7), (byte) (40 + suffix)};
						meta.setString(1, address); meta.setInt(2, height); meta.setBytes(3, Crypto.digest(bytes)); meta.addBatch();
						data.setString(1, address); data.setInt(2, height); data.setBytes(3, bytes); data.addBatch();
					}
				}
				meta.executeBatch(); data.executeBatch();
			}
			c.commit();

			HSQLDBATStoreMigration.migrate(c);
			try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(
					"SELECT payload_bytes FROM ATBlockPayloads WHERE block_height=" + heights)) {
				assertTrue(r.next());
				java.util.Map<String, ATStateBundle.Entry> entries = ATStateBundle.deserialize(r.getBytes(1));
				assertEquals(3, entries.size());
				assertArrayEquals(new byte[]{(byte) (heights % 7), 42}, entries.get("at").data);
				assertArrayEquals(new byte[]{(byte) (heights % 7), 42}, entries.get("at2").data);
				assertArrayEquals(new byte[]{(byte) (heights % 7), 43}, entries.get("at3").data);
			}
			HSQLDBATStoreMigration.validateActiveStore(c);
		}
	}

	private static void stageMigration(Connection c) throws SQLException {
		HSQLDBATStoreMigration.migrate(c);
	}

    @Test
    public void preMigrationBlockCanBeOrphanedAfterCutover() throws Exception {
        try (Connection c = legacy(3)) {
            stageMigration(c);
            byte[] expectedParentHash;
            try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(
                    "SELECT payload_hash FROM ATStates WHERE AT_address='at' AND height=2")) {
                assertTrue(rs.next());
                expectedParentHash = rs.getBytes(1);
            }

            Security.addProvider(new BouncyCastleProvider());
            Settings.fileInstance("src/test/resources/test-settings-v2.json");
            HSQLDBRepository repository = new HSQLDBRepository(c);
            new HSQLDBATStore(repository).deleteBlockResult(3);

            assertEquals(2, count(c, "SELECT latest_state_height FROM ATCurrentState WHERE AT_address='at'"));
            try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(
                    "SELECT payload_hash FROM ATCurrentState WHERE AT_address='at'")) {
                assertTrue(rs.next());
                assertArrayEquals(expectedParentHash, rs.getBytes(1));
            }
            assertEquals(0, count(c, "SELECT COUNT(*) FROM ATStates WHERE height=3"));
            c.rollback();
        }
    }

	@Test
	public void sleepingAtParentBelowTrimFrontierSurvivesMigrationAndOrphan() throws Exception {
		try (Connection c = legacy(20)) {
			try (Statement s = c.createStatement()) {
				s.executeUpdate("UPDATE DatabaseInfo SET AT_trim_height=10");
				s.executeUpdate("DELETE FROM ATStatesData WHERE height NOT IN(5,20)");
				s.executeUpdate("DELETE FROM ATStates WHERE height NOT IN(5,20)");
			}
			c.commit();
			HSQLDBATStoreMigration.migrate(c);

			assertEquals(5, count(c, "SELECT previous_state_height FROM ATCurrentState WHERE AT_address='at'"));
			assertEquals(5, count(c, "SELECT rollback_state_height FROM ATCurrentState WHERE AT_address='at'"));

			Security.addProvider(new BouncyCastleProvider());
			Settings.fileInstance("src/test/resources/test-settings-v2.json");
			HSQLDBRepository repository = new HSQLDBRepository(c);
			assertEquals(1, new HSQLDBATRepository(repository).trimAtStates(2, 10, 100));
			new HSQLDBATRepository(repository).pruneAtStates(2, 10);
			assertEquals(0, count(c, "SELECT COUNT(*) FROM ATBlockPayloads WHERE block_height=5"));
			assertEquals(1, count(c, "SELECT COUNT(*) FROM ATRetainedStatePayloads WHERE AT_address='at' AND state_height=5"));
			assertEquals(1, count(c, "SELECT COUNT(*) FROM ATStates WHERE AT_address='at' AND height=5"));

			new HSQLDBATStore(repository).deleteBlockResult(20);
			assertEquals(5, count(c, "SELECT latest_state_height FROM ATCurrentState WHERE AT_address='at'"));
			assertEquals(1, count(c, "SELECT COUNT(*) FROM ATRetainedStatePayloads WHERE AT_address='at' AND state_height=5"));
			assertEquals(0, count(c, "SELECT COUNT(*) FROM ATBlockPayloads WHERE block_height=20"));
			c.rollback();
		}
	}

	@Test
	public void trimmerAdvancesOneCompactRollbackBoundaryWithoutLeakingOldBundles() throws Exception {
		try (Connection c = legacy(20)) {
			HSQLDBATStoreMigration.migrate(c);
			Security.addProvider(new BouncyCastleProvider());
			Settings.fileInstance("src/test/resources/test-settings-v2.json");
			HSQLDBRepository repository = new HSQLDBRepository(c);
			HSQLDBATRepository ats = new HSQLDBATRepository(repository);

			assertEquals(9, ats.trimAtStates(2, 10, 100));
			assertEquals(10, count(c, "SELECT rollback_state_height FROM ATCurrentState WHERE AT_address='at'"));
			assertEquals(0, count(c, "SELECT COUNT(*) FROM ATBlockPayloads WHERE block_height=10"));
			assertEquals(1, count(c, "SELECT COUNT(*) FROM ATRetainedStatePayloads WHERE AT_address='at' AND state_height=10"));

			assertEquals(5, ats.trimAtStates(11, 15, 100));
			assertEquals(15, count(c, "SELECT rollback_state_height FROM ATCurrentState WHERE AT_address='at'"));
			assertEquals(0, count(c, "SELECT COUNT(*) FROM ATBlockPayloads WHERE block_height=10 AND payload_bytes IS NOT NULL"));
			assertEquals(0, count(c, "SELECT COUNT(*) FROM ATBlockPayloads WHERE block_height=15"));
			assertEquals(0, count(c, "SELECT COUNT(*) FROM ATRetainedStatePayloads WHERE AT_address='at' AND state_height=10"));
			assertEquals(1, count(c, "SELECT COUNT(*) FROM ATRetainedStatePayloads WHERE AT_address='at' AND state_height=15"));
			c.rollback();
		}
	}

	@Test
	public void boundedTrimReportsWhetherCurrentRangeIsComplete() throws Exception {
		try (Connection c = legacy(20)) {
			HSQLDBATStoreMigration.migrate(c);
			Security.addProvider(new BouncyCastleProvider());
			Settings.fileInstance("src/test/resources/test-settings-v2.json");
			HSQLDBATRepository ats = new HSQLDBATRepository(new HSQLDBRepository(c));

			ATRepository.ATTrimResult partial = ats.trimAtStatesDetailed(2, 15, 5);
			assertTrue(partial.getTrimmed() > 0);
			assertFalse(partial.isRangeComplete());

			// The main trim cursor is already caught up (an intentionally empty range),
			// but historical payload cleanup must still make progress.
			ATRepository.ATTrimResult cleanupOnly = ats.trimAtStatesDetailed(30_000, 29_999, 100);
			assertTrue(cleanupOnly.isRangeComplete());
			assertTrue(cleanupOnly.getTrimmed() > 0);
			c.rollback();
		}
	}

	@Test
	public void releasedSharedBundleIsRevisitedAndAssignedToEveryAtThatNeedsIt() throws Exception {
		try (Connection c = legacy(20)) {
			byte[] code = {1, 2, 3};
			try (PreparedStatement at = c.prepareStatement(
					"INSERT INTO ATs VALUES('at2',?,2,2,0,?,?,0,FALSE,NULL,FALSE,FALSE,FALSE,NULL,NULL)")) {
				at.setBytes(1, new byte[32]); at.setBytes(2, code); at.setBytes(3, Crypto.digest(code)); at.executeUpdate();
			}
			try (PreparedStatement meta = c.prepareStatement("INSERT INTO ATStates VALUES('at2',?,?,0,FALSE,NULL)");
				 PreparedStatement data = c.prepareStatement("INSERT INTO ATStatesData VALUES('at2',?,?)")) {
				for (int height : new int[]{10, 20}) {
					byte[] bytes = {(byte) height, 99};
					meta.setInt(1, height); meta.setBytes(2, Crypto.digest(bytes)); meta.addBatch();
					data.setInt(1, height); data.setBytes(2, bytes); data.addBatch();
				}
				meta.executeBatch(); data.executeBatch();
			}
			c.commit();
			HSQLDBATStoreMigration.migrate(c);
			Security.addProvider(new BouncyCastleProvider());
			Settings.fileInstance("src/test/resources/test-settings-v2.json");
			HSQLDBATRepository ats = new HSQLDBATRepository(new HSQLDBRepository(c));

			ats.trimAtStates(2, 10, 100);
			assertEquals(0, count(c, "SELECT COUNT(*) FROM ATBlockPayloads WHERE block_height=10"));
			assertEquals(2, count(c, "SELECT COUNT(*) FROM ATRetainedStatePayloads WHERE state_height=10"));
			ats.trimAtStates(11, 15, 100);
			assertEquals(1, count(c, "SELECT COUNT(*) FROM ATRetainedStatePayloads WHERE state_height=10"));
			assertEquals(10, count(c, "SELECT rollback_state_height FROM ATCurrentState WHERE AT_address='at2'"));
			assertNotNull(ats.getStatePayload("at2", 10,
					payloadHash(c, "at2", "rollback_payload_hash")));
			c.rollback();
		}
	}

	private static byte[] payloadHash(Connection c, String address, String column) throws SQLException {
		try (PreparedStatement statement = c.prepareStatement("SELECT " + column + " FROM ATCurrentState WHERE AT_address=?")) {
			statement.setString(1, address);
			try (ResultSet rows = statement.executeQuery()) {
				assertTrue(rows.next());
				return rows.getBytes(1);
			}
		}
	}

	@Test
	public void historicalCleanupSweepAdvancesInBoundedWindows() throws Exception {
		try (Connection c = legacy(20)) {
			HSQLDBATStoreMigration.migrate(c);
			Security.addProvider(new BouncyCastleProvider());
			Settings.fileInstance("src/test/resources/test-settings-v2.json");
			HSQLDBATRepository ats = new HSQLDBATRepository(new HSQLDBRepository(c));

			ats.trimAtStates(30_000, 29_999, 100);
			assertEquals(10_002, count(c, "SELECT cleanup_height FROM ATStoreInfo"));
			ats.trimAtStates(30_000, 29_999, 100);
			assertEquals(20_002, count(c, "SELECT cleanup_height FROM ATStoreInfo"));
			c.rollback();
		}
	}

	@Test
	public void bundleDeletionAndRetainedPayloadRollBackTogether() throws Exception {
		try (Connection c = legacy(20)) {
			HSQLDBATStoreMigration.migrate(c);
			Security.addProvider(new BouncyCastleProvider());
			Settings.fileInstance("src/test/resources/test-settings-v2.json");
			HSQLDBATRepository ats = new HSQLDBATRepository(new HSQLDBRepository(c));

			ats.trimAtStates(2, 10, 100);
			assertEquals(0, count(c, "SELECT COUNT(*) FROM ATBlockPayloads WHERE block_height=10"));
			assertEquals(1, count(c, "SELECT COUNT(*) FROM ATRetainedStatePayloads WHERE state_height=10"));

			c.rollback();
			assertEquals(1, count(c, "SELECT COUNT(*) FROM ATBlockPayloads WHERE block_height=10"));
			assertEquals(0, count(c, "SELECT COUNT(*) FROM ATRetainedStatePayloads"));
		}
	}

	@Test
	public void mismatchedPointerCannotDeleteItsBundle() throws Exception {
		try (Connection c = legacy(20)) {
			HSQLDBATStoreMigration.migrate(c);
			Security.addProvider(new BouncyCastleProvider());
			Settings.fileInstance("src/test/resources/test-settings-v2.json");
			try (Statement statement = c.createStatement()) {
				statement.executeUpdate("UPDATE ATCurrentState SET latest_state_height=10,payload_hash=X'00'");
			}
			try {
				new HSQLDBATRepository(new HSQLDBRepository(c)).trimAtStates(2, 10, 100);
				fail("corrupt pointer must stop cleanup");
			} catch (org.qortal.repository.DataException expected) {
				c.rollback();
			}
			assertEquals(1, count(c, "SELECT COUNT(*) FROM ATBlockPayloads WHERE block_height=10"));
			assertEquals(0, count(c, "SELECT COUNT(*) FROM ATRetainedStatePayloads"));
		}
	}

	@Test
	public void interimFormatUpgradesWithoutPayloadRewrite() throws Exception {
		try (Connection c = legacy(20)) {
			HSQLDBATStoreMigration.migrate(c);
			try (Statement statement = c.createStatement()) {
				statement.execute("DROP TABLE ATRetainedStatePayloads");
				statement.executeUpdate("UPDATE ATStoreInfo SET format_version=4,cleanup_height=999");
			}
			c.commit();

			HSQLDBATStoreMigration.validateActiveStore(c);
			assertEquals(5, count(c, "SELECT format_version FROM ATStoreInfo"));
			assertEquals(2, count(c, "SELECT cleanup_height FROM ATStoreInfo"));
			assertEquals(1, count(c, "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
					+ "WHERE TABLE_NAME='ATRETAINEDSTATEPAYLOADS'"));
			assertEquals(20, count(c, "SELECT COUNT(*) FROM ATBlockPayloads"));
		}
	}

    @Test
    public void resumesAfterCommittedChunk() throws Exception {
        for (int failurePoint : new int[]{3,5,7,10}) {
            try (Connection c = legacy(4100)) {
                AtomicInteger commits = new AtomicInteger();
                Connection failing = (Connection)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{Connection.class},(proxy,method,args)->{
                    try {
                        Object result = method.invoke(c,args);
                        if (method.getName().equals("commit") && commits.incrementAndGet()==failurePoint)
                            throw new SQLException("simulated interruption after committed migration chunk");
                        return result;
                    } catch (InvocationTargetException e) { throw e.getCause(); }
                });
                try { HSQLDBATStoreMigration.migrate(failing); fail("interruption not reached"); }
                catch (SQLException expected) { assertTrue(expected.getMessage().contains("simulated interruption")); }
                c.rollback();
                HSQLDBATStoreMigration.migrate(c);
                assertMigrated(c,4100);
            }
        }
    }

    @Test
    public void resumesAfterEachCutoverStatement() throws Exception {
        for (int failurePoint = 1; failurePoint <= 5; ++failurePoint) {
            try (Connection c = legacy(2)) {
                int failAt = failurePoint;
                AtomicInteger ddl = new AtomicInteger();
                Connection failing = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class}, (proxy,method,args) -> {
                    try {
                        Object value = method.invoke(c,args);
                        if (!method.getName().equals("createStatement")) return value;
                        Statement statement = (Statement)value;
                        return Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{Statement.class},(p,m,a) -> {
                            try {
                                Object result = m.invoke(statement,a);
                                if (m.getName().equals("execute") && a[0] instanceof String) {
                                    String sql = (String)a[0];
                                    boolean cutover = sql.startsWith("ALTER TABLE ATs RENAME") || sql.startsWith("ALTER TABLE ATStatesData RENAME")
                                            || sql.startsWith("CREATE VIEW") || sql.startsWith("DROP INDEX IF EXISTS ATIndex")
                                            || sql.startsWith("DROP TABLE IF EXISTS LegacyATStatesData");
                                    if (cutover && ddl.incrementAndGet() == failAt) throw new SQLException("cutover interrupted");
                                }
                                return result;
                            } catch (InvocationTargetException e) { throw e.getCause(); }
                        });
                    } catch (InvocationTargetException e) { throw e.getCause(); }
                });
                try { HSQLDBATStoreMigration.migrate(failing); fail("cutover interruption not reached at " + failurePoint); }
                catch (SQLException e) { assertEquals("cutover interrupted",e.getMessage()); }
                c.rollback();
                HSQLDBATStoreMigration.migrate(c);
                assertMigrated(c,2);
            }
        }
    }

    @Test
    public void recipientSeekUsesCompositePositionIndex() throws Exception {
        try (Connection c = legacy(2)) {
            HSQLDBATStoreMigration.migrate(c);
            for (String predicate : new String[]{"block_height=7 AND block_sequence>0", "block_height>7"}) {
                StringBuilder plan = new StringBuilder();
                try (Statement s = c.createStatement(); ResultSet r = s.executeQuery("EXPLAIN PLAN FOR SELECT block_height,block_sequence,signature "
                        + "FROM ATRecipientTransactions WHERE recipient='at' AND " + predicate + " ORDER BY block_height,block_sequence LIMIT 1")) {
                    while (r.next()) plan.append(r.getString(1));
                }
                assertTrue(plan.toString(),plan.toString().contains("ATRECIPIENTPOSITIONINDEX"));
                System.out.println("AT recipient seek plan: " + plan);
            }
        }
    }

    @Test
    public void fileMigrationReopens() throws Exception {
        java.nio.file.Path directory = java.nio.file.Files.createTempDirectory("qortal-at-backup-");
        String url = "jdbc:hsqldb:file:" + directory.resolve("repository");
        try {
            try (Connection c = legacy(10,url)) {
                c.commit();
                HSQLDBATStoreMigration.migrate(c);
                try (Statement s = c.createStatement()) { s.execute("SHUTDOWN"); }
            }
            try (Connection c = DriverManager.getConnection(url+";ifexists=true")) {
				assertEquals(5, count(c, "SELECT format_version FROM ATStoreInfo"));
				assertEquals(0, count(c, "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='ATSTATEPAYLOADS'"));
				assertEquals(1, count(c, "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='ATRETAINEDSTATEPAYLOADS'"));
                HSQLDBATStoreMigration.validateActiveStore(c);
                try (Statement s = c.createStatement()) { s.execute("SHUTDOWN"); }
            }
        } finally { removeDirectory(directory); }
    }

    @Test
    public void resumesAfterAbruptJvmExit() throws Exception {
        java.nio.file.Path directory = java.nio.file.Files.createTempDirectory("qortal-at-crash-");
        String url = "jdbc:hsqldb:file:" + directory.resolve("repository");
        try {
            try (Connection c = legacy(4100,url); Statement s = c.createStatement()) { s.execute("SHUTDOWN"); }
            Process child = new ProcessBuilder(System.getProperty("java.home")+"/bin/java","-cp",System.getProperty("java.class.path"),
                    CrashMigration.class.getName(),url).redirectErrorStream(true).redirectOutput(directory.resolve("child.log").toFile()).start();
            assertTrue("crash fixture timed out",child.waitFor(45,java.util.concurrent.TimeUnit.SECONDS));
            assertEquals(83,child.exitValue());
            try (Connection c = DriverManager.getConnection(url+";ifexists=true")) {
                HSQLDBATStoreMigration.migrate(c);
                assertMigrated(c,4100);
                try (Statement s = c.createStatement()) { s.execute("SHUTDOWN"); }
            }
        } finally { removeDirectory(directory); }
    }

    public static class CrashMigration {
        public static void main(String[] args) throws Exception {
            Connection c = DriverManager.getConnection(args[0]+";ifexists=true");
            AtomicInteger commits = new AtomicInteger();
            Connection failing = (Connection) Proxy.newProxyInstance(CrashMigration.class.getClassLoader(),new Class<?>[]{Connection.class},(proxy,method,parameters)->{
                try {
                    Object result = method.invoke(c,parameters);
                    if (method.getName().equals("commit") && commits.incrementAndGet()==5) Runtime.getRuntime().halt(83);
                    return result;
                } catch (InvocationTargetException e) { throw e.getCause(); }
            });
            HSQLDBATStoreMigration.migrate(failing);
            throw new AssertionError("crash point not reached");
        }
    }

	@Test
	public void loggingOffRuntimeRecoversMemoryAndCachedTablesToSameCheckpoint() throws Exception {
		java.nio.file.Path directory = java.nio.file.Files.createTempDirectory("qortal-at-runtime-crash-");
		String url = "jdbc:hsqldb:file:" + directory.resolve("repository");
		try {
			try (Connection c = legacy(2, url)) {
				HSQLDBATStoreMigration.migrate(c);
				HSQLDBDatabaseUpdates.restoreCheckpointRuntimeMode(c);
				assertEquals("false", property(c, "hsqldb.log_data"));
				try (Statement s = c.createStatement()) { s.execute("SHUTDOWN"); }
			}

			Process child = new ProcessBuilder(System.getProperty("java.home") + "/bin/java", "-cp",
					System.getProperty("java.class.path"), CrashAfterUnloggedCommit.class.getName(), url)
					.redirectErrorStream(true).redirectOutput(directory.resolve("child.log").toFile()).start();
			assertTrue("runtime crash fixture timed out", child.waitFor(30, java.util.concurrent.TimeUnit.SECONDS));
			assertEquals(84, child.exitValue());
			// A halted HSQLDB process leaves a heartbeat lock that expires after roughly ten seconds.
			Thread.sleep(11_000L);

			try (Connection c = DriverManager.getConnection(url + ";ifexists=true")) {
				assertEquals(2, count(c, "SELECT latest_state_height FROM ATCurrentState WHERE AT_address='at'"));
				assertEquals(0, count(c, "SELECT COUNT(*) FROM ATBlockResults WHERE block_height=99"));
				HSQLDBATStoreMigration.validateActiveStore(c);
				try (Statement s = c.createStatement()) { s.execute("SHUTDOWN"); }
			}
		} finally { removeDirectory(directory); }
	}

	public static class CrashAfterUnloggedCommit {
		public static void main(String[] args) throws Exception {
			try (Connection c = DriverManager.getConnection(args[0] + ";ifexists=true")) {
				c.setAutoCommit(false);
				try (Statement s = c.createStatement()) {
					s.executeUpdate("UPDATE ATCurrentState SET latest_state_height=1,payload_hash="
							+ "(SELECT payload_hash FROM ATStates WHERE AT_address='at' AND height=1) WHERE AT_address='at'");
					s.executeUpdate("INSERT INTO ATBlockResults(block_height,result_bytes) VALUES(99,NULL)");
				}
				c.commit();
				Runtime.getRuntime().halt(84);
			}
		}
	}

	private static String property(Connection c, String name) throws SQLException {
		try (PreparedStatement s = c.prepareStatement(
				"SELECT PROPERTY_VALUE FROM INFORMATION_SCHEMA.SYSTEM_PROPERTIES WHERE PROPERTY_NAME=?")) {
			s.setString(1, name);
			try (ResultSet r = s.executeQuery()) {
				assertTrue(r.next());
				return r.getString(1);
			}
		}
	}

    private static void removeDirectory(java.nio.file.Path directory) throws Exception {
        try (java.util.stream.Stream<java.nio.file.Path> paths = java.nio.file.Files.walk(directory)) {
            for (java.nio.file.Path path : (Iterable<java.nio.file.Path>)paths.sorted(java.util.Comparator.reverseOrder())::iterator)
                java.nio.file.Files.deleteIfExists(path);
        }
    }

    @Test
    public void contractLookupUsesIndexedPredicates() throws Exception {
        try (Connection c = legacy(2)) {
            HSQLDBATStoreMigration.migrate(c);
            StringBuilder plan = new StringBuilder();
            try (Statement s = c.createStatement(); ResultSet r = s.executeQuery("EXPLAIN PLAN FOR SELECT creator,code_bytes "
                    + "FROM ATDefinitions ATs JOIN ATCurrentState USING(AT_address) JOIN ATCode USING(code_hash) "
                    + "WHERE ATs.AT_address='at' LIMIT 1")) {
                while (r.next()) plan.append(r.getString(1));
            }
            assertFalse(plan.toString(), plan.toString().contains("access=FULL SCAN"));
        }
    }

    @Test
    public void rejectsConsistentButStaleHead() throws Exception {
        try (Connection c = legacy(10)) {
            HSQLDBATStoreMigration.migrate(c);
            HSQLDBATStoreMigration.validateActiveStore(c);
            try (Statement s = c.createStatement()) {
                s.execute("UPDATE ATCurrentState SET latest_state_height=9,payload_hash="
                        + "(SELECT payload_hash FROM ATStates WHERE AT_address='at' AND height=9)");
            }
            try { HSQLDBATStoreMigration.validateActiveStore(c); fail("accepted stale head"); }
            catch (SQLException expected) { assertTrue(expected.getMessage().contains("current state/history/schedule inconsistency")); }
        }
    }

    @Test
    public void refusesCorruptLegacyPayloadBeforeCutover() throws Exception {
        try (Connection c = legacy(2)) {
            try (Statement s = c.createStatement()) { s.execute("UPDATE ATStatesData SET state_data=X'FF' WHERE height=2"); }
            c.commit();
            try { HSQLDBATStoreMigration.migrate(c); fail("accepted corrupt legacy data"); }
            catch (SQLException expected) {
				assertTrue(expected.toString(), expected.getMessage().contains("Unable to encode AT payload bundle"));
			}
            c.rollback();
            assertEquals(1,count(c,"SELECT phase FROM ATStoreInfo"));
            assertEquals(1,count(c,"SELECT COUNT(*) FROM ATs"));
        }
    }

}
