package org.qortal.repository.hsqldb;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.qortal.crypto.Crypto;
import org.qortal.at.ATStateBundle;
import org.qortal.repository.DataException;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Startup-only, resumable AT store migration. Every cursor commits with its data. */
public final class HSQLDBATStoreMigration {
	private static final Logger LOGGER = LogManager.getLogger(HSQLDBATStoreMigration.class);
	private static final int CHUNK = 2048;
	private static final int PAYLOAD_BUNDLE_CHUNK = 16_384;
	private static final int ACTIVE_FORMAT_VERSION = 5;

    private HSQLDBATStoreMigration() { }

    public static void migrate(Connection connection) throws SQLException {
        connection.setAutoCommit(false);
        // Schema 29 disabled redo logging. Synchronous delay alone cannot make a commit durable.
        execute(connection, "SET FILES LOG TRUE");
        execute(connection, "SET FILES WRITE DELAY 0");
        // Capture any schema/data written by older migrations with logging disabled.
        // Enabling the log alone cannot recover those earlier unlogged changes.
        connection.commit();
        execute(connection, "CHECKPOINT");
        execute(connection, "CREATE TABLE IF NOT EXISTS ATStoreInfo (id INTEGER PRIMARY KEY CHECK(id = 1), "
                + "phase INTEGER NOT NULL, last_address QortalAddress, last_height INTEGER, last_signature Signature, "
				+ "tip_signature BlockSignature, format_version INTEGER NOT NULL, cleanup_height INTEGER DEFAULT 2 NOT NULL)");
		execute(connection, "INSERT INTO ATStoreInfo (id,phase,last_address,last_height,last_signature,tip_signature,format_version) "
				+ "SELECT 1, 0, NULL, 0, NULL, NULL, 0 FROM (VALUES(0)) "
                + "WHERE NOT EXISTS (SELECT 1 FROM ATStoreInfo WHERE id = 1)");
        connection.commit();
		int phase = phase(connection);
		int formatVersion = formatVersion(connection);
		if (formatVersion != 0 && formatVersion != 4 && formatVersion != ACTIVE_FORMAT_VERSION)
			throw new SQLException("Unsupported AT store format " + formatVersion);
		if (formatVersion != 0 && phase != 6)
			throw new SQLException("Completed AT store format has incomplete migration phase " + phase);
		if (phase >= 6) { finalizeStore(connection); return; }

		execute(connection, "CREATE TABLE IF NOT EXISTS ATCode (code_hash VARBINARY(32) PRIMARY KEY, code_bytes ATCode NOT NULL)");
        // Signature is the key: old confirmed records can have NULL block_sequence.
        execute(connection, "CREATE TABLE IF NOT EXISTS ATRecipientTransactions (signature Signature PRIMARY KEY, "
                + "recipient QortalAddress NOT NULL, block_height INTEGER NOT NULL, block_sequence INTEGER)");
        if (!tableExists(connection, "ATCURRENTSTATE")) execute(connection, "CREATE MEMORY TABLE ATCurrentState (AT_address QortalAddress PRIMARY KEY, "
                + "latest_state_height INTEGER, payload_hash ATStateHash, is_sleeping BOOLEAN NOT NULL, sleep_until_height INTEGER, "
                + "is_finished BOOLEAN NOT NULL, had_fatal_error BOOLEAN NOT NULL, is_frozen BOOLEAN NOT NULL, frozen_balance QortalAmount, "
                + "sleep_until_message_timestamp BIGINT, cursor_height INTEGER, cursor_sequence INTEGER, "
				+ "schedule_class INTEGER NOT NULL, message_pending BOOLEAN NOT NULL DEFAULT FALSE, "
				+ "previous_state_height INTEGER, previous_payload_hash ATStateHash, "
				+ "rollback_state_height INTEGER, rollback_payload_hash ATStateHash, "
                + "FOREIGN KEY (AT_address) REFERENCES ATs(AT_address) ON DELETE CASCADE)");
        ensureSplitTables(connection);
        if (!columnExists(connection, "ATSTATES", "PAYLOAD_HASH"))
            execute(connection, "ALTER TABLE ATStates ADD payload_hash ATStateHash");
        connection.commit();

		if (phase == 0) { copyCode(connection); advance(connection, 1); phase = 1; }
		if (phase == 1) { copyPayloadBundles(connection); advance(connection, 2); phase = 2; }
        if (phase == 2) { copyRecipients(connection); advance(connection, 3); phase = 3; }
        index(connection, "ATRecipientPositionIndex", "ATRecipientTransactions", "recipient, block_height, block_sequence");
        index(connection, "ATRecipientHeightIndex", "ATRecipientTransactions", "block_height, block_sequence");
        if (phase == 3) { copyHeads(connection); advance(connection, 4); phase = 4; }
        index(connection, "ATReadyIndex", "ATCurrentState", "schedule_class, message_pending, AT_address");
        index(connection, "ATWakeHeightIndex", "ATCurrentState", "schedule_class, sleep_until_height, AT_address");
        index(connection, "ATHeadPayloadIndex", "ATCurrentState", "payload_hash");
        if (phase == 4) {
            requireZero(connection, "SELECT COUNT(*) FROM ATs a LEFT JOIN ATCurrentState s USING(AT_address) "
                    + "WHERE s.AT_address IS NULL OR s.latest_state_height IS NULL OR s.payload_hash IS NULL", "missing current AT state");
			requireZero(connection, "SELECT COUNT(*) FROM ATStatesData d JOIN ATStates s USING(AT_address,height) "
					+ "WHERE s.payload_hash IS NULL", "missing migrated payload link");
			requireZero(connection, "SELECT COUNT(*) FROM ATStatesData d LEFT JOIN ATStates s USING(AT_address,height) WHERE s.AT_address IS NULL", "payload without state history");
			requireZero(connection, "SELECT COUNT(*) FROM ATStatesData d LEFT JOIN ATBlockPayloads b ON b.block_height=d.height "
					+ "AND b.payload_bytes IS NOT NULL WHERE b.block_height IS NULL", "payload without block bundle");
            advance(connection, 5); phase = 5;
        }
        if (phase == 5) {
            // Keep the existing definition table and its foreign keys intact. HSQLDB
            // rewrites cached tables for both DROP COLUMN and foreign-key replacement;
            // either operation makes this otherwise bounded migration unnecessarily huge.
            if (!tableExists(connection, "ATDEFINITIONS")) execute(connection, "ALTER TABLE ATs RENAME TO ATDefinitions");
            if (!tableExists(connection, "LEGACYATSTATESDATA") && tableExists(connection, "ATSTATESDATA"))
                execute(connection, "ALTER TABLE ATStatesData RENAME TO LegacyATStatesData");
            if (!viewExists(connection, "ATS")) execute(connection, "CREATE VIEW ATs AS SELECT d.AT_address, d.creator, d.created_when, d.version, d.asset_id, "
                    + "c.code_bytes, d.code_hash, d.creation_group_id, s.is_sleeping, s.sleep_until_height, s.is_finished, "
                    + "s.had_fatal_error, s.is_frozen, s.frozen_balance, s.sleep_until_message_timestamp "
                    + "FROM ATDefinitions d JOIN ATCode c ON c.code_hash = d.code_hash JOIN ATCurrentState s ON s.AT_address = d.AT_address");
			execute(connection, "DROP INDEX IF EXISTS ATIndex");
            index(connection, "ATDefinitionOrderIndex", "ATDefinitions", "created_when, AT_address");
            execute(connection, "UPDATE ATStoreInfo SET tip_signature = (SELECT signature FROM Blocks ORDER BY height DESC LIMIT 1) WHERE id = 1");
            advance(connection, 6);
			finalizeStore(connection);
			LOGGER.info("AT store migration complete");
		}
	}

	/**
	 * Convert retained legacy payload rows directly into their final block bundles. The
	 * cursor advances only past a complete height, so a committed cursor always describes
	 * complete bundles and an interrupted run never has to repair a partial block.
	 */
	private static void copyPayloadBundles(Connection c) throws SQLException {
		int cursorHeight = height(c);
		while (true) {
			Map<Integer, Map<String, ATStateBundle.Entry>> byHeight = new LinkedHashMap<>();
			List<Object[]> links = new ArrayList<>();
			int endHeight = cursorHeight;
			String endAddress = null;

			try (PreparedStatement ps = c.prepareStatement(
					"SELECT d.height,d.AT_address,s.state_hash,d.state_data FROM ATStatesData d "
							+ "JOIN ATStates s USING(AT_address,height) WHERE d.height>? "
							+ "ORDER BY d.height,d.AT_address LIMIT " + PAYLOAD_BUNDLE_CHUNK)) {
				ps.setInt(1, cursorHeight);
				try (ResultSet rs = ps.executeQuery()) {
					while (rs.next()) {
						endHeight = addPayloadRow(byHeight, links, rs);
						endAddress = rs.getString(2);
					}
				}
			}
			if (links.isEmpty()) return;

			// The bounded query can end partway through a busy block. Complete that height
			// before writing anything so the durable cursor always lands between blocks.
			try (PreparedStatement ps = c.prepareStatement(
					"SELECT d.height,d.AT_address,s.state_hash,d.state_data FROM ATStatesData d "
							+ "JOIN ATStates s USING(AT_address,height) WHERE d.height=? AND d.AT_address>? "
							+ "ORDER BY d.AT_address")) {
				ps.setInt(1, endHeight);
				ps.setString(2, endAddress);
				try (ResultSet rs = ps.executeQuery()) {
					while (rs.next()) addPayloadRow(byHeight, links, rs);
				}
			}

			writePayloadBundles(c, byHeight);
			try (PreparedStatement link = c.prepareStatement(
					"UPDATE ATStates SET payload_hash=? WHERE AT_address=? AND height=?")) {
				for (Object[] row : links) {
					link.setBytes(1, (byte[]) row[0]);
					link.setString(2, (String) row[1]);
					link.setInt(3, (Integer) row[2]);
					link.addBatch();
				}
				link.executeBatch();
			}
			checkpoint(c, null, endHeight, null);
			cursorHeight = endHeight;
			LOGGER.info("AT direct payload migration committed {} rows through height {}", links.size(), cursorHeight);
		}
	}

	private static int addPayloadRow(Map<Integer, Map<String, ATStateBundle.Entry>> byHeight,
			List<Object[]> links, ResultSet rs) throws SQLException {
		int height = rs.getInt(1);
		String address = rs.getString(2);
		byte[] hash = rs.getBytes(3);
		byte[] data = rs.getBytes(4);
		Map<String, ATStateBundle.Entry> block = byHeight.computeIfAbsent(height, ignored -> new LinkedHashMap<>());
		ATStateBundle.Entry previous = block.putIfAbsent(address, new ATStateBundle.Entry(hash, data));
		if (previous != null)
			throw new SQLException("Duplicate legacy AT payload at height " + height + " for " + address);
		links.add(new Object[]{hash, address, height});
		return height;
	}

	private static void writePayloadBundles(Connection c,
			Map<Integer, Map<String, ATStateBundle.Entry>> bundles) throws SQLException {
		if (bundles.isEmpty()) return;
		int firstHeight = bundles.keySet().iterator().next();
		int lastHeight = firstHeight;
		for (Integer height : bundles.keySet()) lastHeight = height;
		try (PreparedStatement check = c.prepareStatement(
				"SELECT COUNT(*) FROM ATBlockPayloads WHERE block_height BETWEEN ? AND ?")) {
			check.setInt(1, firstHeight); check.setInt(2, lastHeight);
			try (ResultSet rs = check.executeQuery()) {
				rs.next();
				if (rs.getInt(1) != 0)
					throw new SQLException("AT migration cursor encountered an already-written block range");
			}
		}

		try (PreparedStatement insert = c.prepareStatement(
				"INSERT INTO ATBlockPayloads(block_height,payload_bytes) VALUES(?,?)")) {
			for (Map.Entry<Integer, Map<String, ATStateBundle.Entry>> item : bundles.entrySet()) {
				byte[] encoded;
				try { encoded = ATStateBundle.serialize(item.getValue()); }
				catch (DataException e) {
					throw new SQLException("Unable to encode AT payload bundle at height " + item.getKey(), e);
				}
				insert.setInt(1, item.getKey());
				insert.setBytes(2, encoded);
				insert.addBatch();
			}
			insert.executeBatch();
		}
	}

	private static void verifyStoreCutover(Connection c) throws SQLException {
		try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(
				"SELECT tip_signature,(SELECT signature FROM Blocks ORDER BY height DESC LIMIT 1) FROM ATStoreInfo WHERE id=1")) {
			if (!r.next() || !Arrays.equals(r.getBytes(1), r.getBytes(2)))
				throw new SQLException("Chain tip changed during AT store migration");
		}

		try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(
				"SELECT AT_address,latest_state_height,payload_hash,previous_state_height,previous_payload_hash,"
						+ "rollback_state_height,rollback_payload_hash FROM ATCurrentState")) {
			while (r.next()) {
				verifyBundlePointer(c, r.getString(1), (Integer) r.getObject(2), r.getBytes(3), "current");
				verifyBundlePointer(c, r.getString(1), (Integer) r.getObject(4), r.getBytes(5), "previous");
				verifyBundlePointer(c, r.getString(1), (Integer) r.getObject(6), r.getBytes(7), "rollback boundary");
			}
		}
	}

	private static void verifyBundlePointer(Connection c, String address, Integer height, byte[] hash, String kind) throws SQLException {
		if (height == null || hash == null) return;
		try (PreparedStatement ps = c.prepareStatement(
				"SELECT payload_bytes FROM ATBlockPayloads WHERE block_height=?")) {
			ps.setInt(1, height);
			try (ResultSet rs = ps.executeQuery()) {
				if (!rs.next()) throw new SQLException("Missing " + kind + " AT payload bundle for " + address);
				byte[] bundle = rs.getBytes(1);
				try {
					ATStateBundle.Entry entry = bundle == null ? null : ATStateBundle.deserialize(bundle).get(address);
					if (entry == null || !Arrays.equals(hash, entry.hash))
						throw new SQLException("Missing " + kind + " AT payload for " + address);
				} catch (DataException e) { throw new SQLException("Invalid AT payload bundle at height " + height, e); }
			}
		}
	}

	private static void finalizeStore(Connection connection) throws SQLException {
		int format = formatVersion(connection);
		if (format == 0) {
			if (phase(connection) != 6)
				throw new SQLException("AT store cannot be finalized before migration completes");
			verifyStoreCutover(connection);
			if (tableExists(connection, "LEGACYATSTATESDATA"))
				requireZero(connection, "SELECT COUNT(*) FROM LegacyATStatesData d LEFT JOIN ATBlockPayloads b "
						+ "ON b.block_height=d.height AND b.payload_bytes IS NOT NULL WHERE b.block_height IS NULL",
						"legacy payload without final block bundle");
			ensureRetainedPayloadTable(connection);
			execute(connection, "UPDATE ATStoreInfo SET format_version=" + ACTIVE_FORMAT_VERSION + " WHERE id=1");
			connection.commit();
		} else if (format == 4) {
			// Interim, unreleased stores used block bundles for long-lived pointers. Add a
			// bounded spill table and restart the historical sweep so bundles skipped as
			// "protected" by that format are revisited and detached.
			// Some of those builds used checkpoint-only runtime persistence, so enable the
			// recovery log before publishing the new active-format marker.
			execute(connection, "SET FILES LOG TRUE");
			execute(connection, "SET FILES WRITE DELAY 0");
			connection.commit();
			ensureRetainedPayloadTable(connection);
			execute(connection, "UPDATE ATStoreInfo SET format_version=" + ACTIVE_FORMAT_VERSION
					+ ",cleanup_height=2 WHERE id=1");
			connection.commit();
		} else if (format == ACTIVE_FORMAT_VERSION) {
			if (!tableExists(connection, "ATRETAINEDSTATEPAYLOADS"))
				throw new SQLException("Active AT retained-payload table is missing");
		} else {
			throw new SQLException("Unsupported AT store format " + format);
		}

		// The active-format marker is durable before this restartable physical cleanup.
		execute(connection, "DROP TABLE IF EXISTS LegacyATStatesData");
		connection.commit();
		LOGGER.info("AT direct payload migration complete; block bundles are authoritative");
	}

	private static void ensureRetainedPayloadTable(Connection connection) throws SQLException {
		if (!tableExists(connection, "ATRETAINEDSTATEPAYLOADS")) {
			execute(connection, "CREATE TABLE ATRetainedStatePayloads ("
					+ "AT_address QortalAddress NOT NULL, state_height INTEGER NOT NULL, "
					+ "payload_hash ATStateHash NOT NULL, payload_bytes VARBINARY(16M) NOT NULL, "
					+ "PRIMARY KEY (AT_address,state_height), "
					+ "FOREIGN KEY (AT_address) REFERENCES ATDefinitions(AT_address) ON DELETE CASCADE)");
		}
	}

	private static void ensureSplitTables(Connection connection) throws SQLException {
		// Required by file databases before SET TABLE ... NEW SPACE. This setting is
		// already enabled on normal Qortal schemas, but the migration must also be safe
		// for older databases and file-based recovery/bootstrap tests.
		execute(connection, "SET FILES SPACE TRUE");
		if (!tableExists(connection, "ATBLOCKPAYLOADS")) {
			execute(connection, "CREATE TABLE ATBlockPayloads (block_height INT PRIMARY KEY, payload_bytes VARBINARY(16M))");
			execute(connection, "SET TABLE ATBlockPayloads NEW SPACE");
		}
		if (!tableExists(connection, "ATBLOCKRESULTS")) {
			execute(connection, "CREATE TABLE ATBlockResults (block_height INT PRIMARY KEY, result_bytes VARBINARY(16M))");
			execute(connection, "SET TABLE ATBlockResults NEW SPACE");
		}
		if (!columnExists(connection, "ATBLOCKPAYLOADS", "PAYLOAD_BYTES")
				|| columnExists(connection, "ATBLOCKPAYLOADS", "RESULT_BYTES")
				|| !columnExists(connection, "ATBLOCKRESULTS", "RESULT_BYTES")
				|| columnExists(connection, "ATBLOCKRESULTS", "PAYLOAD_BYTES"))
			throw new SQLException("AT result/payload tables do not match the final schema");
		connection.commit();
	}

	public static void validateActiveStore(Connection c) throws SQLException {
		// This also upgrades databases produced by short-lived development builds whose
		// schema counter is newer than the consolidated release migration.
		if (formatVersion(c) == 4)
			finalizeStore(c);
        try (Statement s = c.createStatement(); ResultSet r = s.executeQuery("SELECT phase,format_version,tip_signature,"
                + "(SELECT signature FROM Blocks ORDER BY height DESC LIMIT 1) FROM ATStoreInfo WHERE id=1")) {
			if (!r.next() || r.getInt(1) != 6 || r.getInt(2) != ACTIVE_FORMAT_VERSION
					|| !Arrays.equals(r.getBytes(3), r.getBytes(4)))
                throw new SQLException("AT store is incomplete, incompatible, or belongs to a different chain tip");
        }
		if (tableExists(c, "ATSTATEPAYLOADS") || viewExists(c, "ATSTATESDATA"))
			throw new SQLException("AT store migration left legacy payload storage active");
		if (!tableExists(c, "ATBLOCKPAYLOADS") || !columnExists(c, "ATBLOCKPAYLOADS", "PAYLOAD_BYTES")
				|| !tableExists(c, "ATBLOCKRESULTS") || !columnExists(c, "ATBLOCKRESULTS", "RESULT_BYTES")
				|| columnExists(c, "ATBLOCKRESULTS", "PAYLOAD_BYTES")
				|| !tableExists(c, "ATRETAINEDSTATEPAYLOADS"))
			throw new SQLException("AT result/payload split store is incomplete");
		requireZero(c,"SELECT COUNT(*) FROM ATDefinitions d LEFT JOIN ATCurrentState c USING(AT_address) "
				+ "LEFT JOIN ATBlockPayloads b ON b.block_height=c.latest_state_height AND b.payload_bytes IS NOT NULL "
				+ "LEFT JOIN ATRetainedStatePayloads r ON r.AT_address=c.AT_address "
				+ "AND r.state_height=c.latest_state_height AND r.payload_hash=c.payload_hash "
				+ "WHERE c.AT_address IS NULL OR (c.payload_hash IS NOT NULL AND b.block_height IS NULL AND r.AT_address IS NULL) "
				+ "OR c.schedule_class<>CASE WHEN c.is_finished THEN 2 WHEN c.sleep_until_message_timestamp IS NULL THEN 0 ELSE 1 END",
				"current state/bundle/schedule inconsistency");
		requireZero(c,"SELECT COUNT(*) FROM ATCurrentState c LEFT JOIN ATBlockPayloads b "
				+ "ON b.block_height=c.rollback_state_height AND b.payload_bytes IS NOT NULL "
				+ "LEFT JOIN ATRetainedStatePayloads r ON r.AT_address=c.AT_address "
				+ "AND r.state_height=c.rollback_state_height AND r.payload_hash=c.rollback_payload_hash "
				+ "WHERE c.rollback_payload_hash IS NOT NULL AND b.block_height IS NULL AND r.AT_address IS NULL",
				"rollback boundary/bundle inconsistency");
		requireZero(c,"SELECT COUNT(*) FROM ATCurrentState c LEFT JOIN ATBlockPayloads b "
				+ "ON b.block_height=c.previous_state_height AND b.payload_bytes IS NOT NULL "
				+ "LEFT JOIN ATRetainedStatePayloads r ON r.AT_address=c.AT_address "
				+ "AND r.state_height=c.previous_state_height AND r.payload_hash=c.previous_payload_hash "
				+ "WHERE c.previous_payload_hash IS NOT NULL AND b.block_height IS NULL AND r.AT_address IS NULL",
				"previous state/bundle inconsistency");
        // Legacy ATStates table is no longer maintained — only validate that remaining rows
        // don't have a height beyond their ATCurrentState entry (may be absent for post-migration ATs)
        requireZero(c,"SELECT COUNT(*) FROM ATDefinitions d "
                + "JOIN ATCurrentState c USING(AT_address) "
                + "JOIN ATStates s ON s.AT_address=d.AT_address AND s.height>c.latest_state_height",
				"current state/history/schedule inconsistency: stale ATStates entries");
    }

    private static void copyCode(Connection c) throws SQLException {
        ATImmutableCache copied = new ATImmutableCache(8L * 1024 * 1024);
        while (true) {
            String last = address(c);
            int count = 0;
            try (PreparedStatement read = c.prepareStatement("SELECT AT_address, code_hash, code_bytes FROM ATs "
                    + "WHERE AT_address > ? ORDER BY AT_address LIMIT " + CHUNK);
                 PreparedStatement insert = c.prepareStatement("INSERT INTO ATCode VALUES (?, ?)")) {
                read.setString(1, last);
                try (ResultSet rs = read.executeQuery()) {
                    while (rs.next()) {
                        last = rs.getString(1);
                        byte[] hash = rs.getBytes(2), bytes = rs.getBytes(3);
                        verify(hash, bytes);
                        if (copied.get(hash) != null) { ++count; continue; }
                        try (PreparedStatement check = c.prepareStatement("SELECT code_bytes FROM ATCode WHERE code_hash = ?")) {
                            check.setBytes(1, hash);
                            try (ResultSet existing = check.executeQuery()) {
                                if (existing.next()) {
                                    if (!Arrays.equals(bytes, existing.getBytes(1))) throw new SQLException("AT code hash collision/corruption");
                                } else { insert.setBytes(1, hash); insert.setBytes(2, bytes); insert.executeUpdate(); }
                            }
                        }
                        copied.put(hash, bytes);
                        ++count;
                    }
                }
            }
            checkpoint(c, last, 0, null);
            if (count < CHUNK) return;
        }
    }

    private static void copyRecipients(Connection c) throws SQLException {
        String[] sources = {"PaymentTransactions", "MessageTransactions", "ATTransactions"};
        int source = height(c);
        byte[] last;
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT last_signature FROM ATStoreInfo WHERE id = 1")) {
            rs.next(); last = rs.getBytes(1);
        }
        for (; source < sources.length; ++source) {
            while (true) {
                int count = 0;
                // Scan only each eligible subtype's primary key, not every QDN/chat transaction.
				String sql = "SELECT p.signature,t.block_height,t.block_sequence,p.recipient FROM " + sources[source]
						+ " p JOIN Transactions t ON t.signature=p.signature JOIN ATs a ON a.AT_address=p.recipient "
						+ "WHERE p.signature > ? AND t.block_height IS NOT NULL AND p.recipient IS NOT NULL "
                        + "ORDER BY p.signature LIMIT " + CHUNK;
                try (PreparedStatement read = c.prepareStatement(sql);
                     PreparedStatement insert = c.prepareStatement("INSERT INTO ATRecipientTransactions VALUES(?,?,?,?)")) {
                    read.setBytes(1, last == null ? new byte[0] : last);
                    try (ResultSet rs = read.executeQuery()) {
                        while (rs.next()) {
                            last = rs.getBytes(1);
                            insert.setBytes(1, last); insert.setString(2, rs.getString(4));
                            insert.setInt(3, rs.getInt(2)); insert.setObject(4, rs.getObject(3)); insert.addBatch();
                            ++count;
                        }
                    }
                    if (count > 0) insert.executeBatch();
                }
                checkpoint(c, "", source, last);
                LOGGER.info("AT recipient backfill: {} committed {} rows", sources[source], count);
                if (count < CHUNK) break;
            }
            last = null;
            checkpoint(c, "", source + 1, null);
        }
    }

    private static void copyHeads(Connection c) throws SQLException {
        // Keyset windows keep the working transaction bounded; correlated queries use the existing state primary key.
        while (true) {
            String last = address(c), end = last;
            int count = 0;
            try (PreparedStatement read = c.prepareStatement("SELECT AT_address FROM ATs WHERE AT_address > ? ORDER BY AT_address LIMIT " + CHUNK)) {
                read.setString(1, last);
                try (ResultSet rs = read.executeQuery()) { while (rs.next()) { end = rs.getString(1); ++count; } }
            }
			try (PreparedStatement insert = c.prepareStatement("INSERT INTO ATCurrentState "
					+ "SELECT a.AT_address, h.height, h.payload_hash, a.is_sleeping, a.sleep_until_height, a.is_finished, "
					+ "a.had_fatal_error, a.is_frozen, a.frozen_balance, a.sleep_until_message_timestamp, NULL, NULL, "
					+ "CASE WHEN a.is_finished THEN 2 WHEN a.sleep_until_message_timestamp IS NULL THEN 0 ELSE 1 END, FALSE,"
					+ "p.height,p.payload_hash,r.height,r.payload_hash "
					+ "FROM ATs a LEFT JOIN LATERAL(SELECT height,payload_hash FROM ATStates WHERE AT_address = a.AT_address "
					+ "AND payload_hash IS NOT NULL ORDER BY AT_address DESC,height DESC LIMIT 1) h ON TRUE "
					+ "LEFT JOIN LATERAL(SELECT height,payload_hash FROM ATStates WHERE AT_address=a.AT_address "
					+ "AND height<h.height AND payload_hash IS NOT NULL ORDER BY AT_address DESC,height DESC LIMIT 1) p ON TRUE "
					+ "LEFT JOIN LATERAL(SELECT height,payload_hash FROM ATStates WHERE AT_address=a.AT_address "
					+ "AND height<=(SELECT AT_trim_height FROM DatabaseInfo) AND payload_hash IS NOT NULL "
					+ "ORDER BY AT_address DESC,height DESC LIMIT 1) r ON TRUE "
					+ "WHERE a.AT_address > ? AND a.AT_address <= ?")) {
                insert.setString(1, last); insert.setString(2, end); insert.executeUpdate();
            }
            try (PreparedStatement read = c.prepareStatement("SELECT AT_address,sleep_until_message_timestamp FROM ATCurrentState "
                    + "WHERE AT_address > ? AND AT_address <= ? AND sleep_until_message_timestamp IS NOT NULL");
                 PreparedStatement update = c.prepareStatement("UPDATE ATCurrentState SET cursor_height = ?, cursor_sequence = ? WHERE AT_address = ?")) {
                read.setString(1, last); read.setString(2, end);
                int cursorCount = 0;
                try (ResultSet rs = read.executeQuery()) {
                    while (rs.next()) {
                        org.ciyam.at.Timestamp cursor = new org.ciyam.at.Timestamp(rs.getLong(2));
                        update.setInt(1, cursor.blockHeight); update.setInt(2, cursor.transactionSequence); update.setString(3, rs.getString(1)); update.addBatch(); ++cursorCount;
                    }
                }
                if (cursorCount > 0) update.executeBatch();
            }
            try (PreparedStatement update = c.prepareStatement("UPDATE ATCurrentState s SET message_pending = EXISTS "
                    + "(SELECT 1 FROM ATRecipientTransactions t WHERE t.recipient = s.AT_address AND "
                    + "(t.block_height > s.cursor_height OR (t.block_height = s.cursor_height AND t.block_sequence > s.cursor_sequence))) "
                    + "WHERE s.AT_address > ? AND s.AT_address <= ? AND s.schedule_class = 1")) {
                update.setString(1, last); update.setString(2, end); update.executeUpdate();
            }
            checkpoint(c, end, 0, null);
            if (count < CHUNK) return;
        }
    }

    private static void verify(byte[] hash, byte[] bytes) throws SQLException {
        if (bytes == null || !Arrays.equals(hash, Crypto.digest(bytes))) throw new SQLException("AT migration found an invalid code/state hash");
    }
    private static int phase(Connection c) throws SQLException {
        try (Statement s = c.createStatement(); ResultSet r = s.executeQuery("SELECT phase FROM ATStoreInfo WHERE id = 1")) { r.next(); return r.getInt(1); }
    }
	private static int formatVersion(Connection c) throws SQLException {
		try (Statement s = c.createStatement(); ResultSet r = s.executeQuery("SELECT format_version FROM ATStoreInfo WHERE id = 1")) { r.next(); return r.getInt(1); }
	}

    private static String address(Connection c) throws SQLException {
        try (Statement s = c.createStatement(); ResultSet r = s.executeQuery("SELECT last_address FROM ATStoreInfo WHERE id = 1")) { r.next(); String a = r.getString(1); return a == null ? "" : a; }
    }
    private static int height(Connection c) throws SQLException {
        try (Statement s = c.createStatement(); ResultSet r = s.executeQuery("SELECT last_height FROM ATStoreInfo WHERE id = 1")) { r.next(); return r.getInt(1); }
    }
    private static void checkpoint(Connection c, String address, int height, byte[] signature) throws SQLException {
        try (PreparedStatement s = c.prepareStatement("UPDATE ATStoreInfo SET last_address = ?, last_height = ?, last_signature = ? WHERE id = 1")) {
            s.setString(1, address); s.setInt(2, height); s.setBytes(3, signature); s.executeUpdate();
        }
        c.commit();
    }
    private static void advance(Connection c, int phase) throws SQLException {
        execute(c, "UPDATE ATStoreInfo SET phase = " + phase + ", last_address = NULL, last_height = 0, last_signature = NULL WHERE id = 1"); c.commit();
    }
    private static void execute(Connection c, String sql) throws SQLException { try (Statement s = c.createStatement()) { s.execute(sql); } }
    private static boolean tableExists(Connection c, String name) throws SQLException {
        try (ResultSet r = c.getMetaData().getTables(null, "PUBLIC", name, new String[]{"TABLE"})) { return r.next(); }
    }
    private static boolean viewExists(Connection c, String name) throws SQLException {
        try (ResultSet r = c.getMetaData().getTables(null, "PUBLIC", name, new String[]{"VIEW"})) { return r.next(); }
    }
    private static boolean columnExists(Connection c, String table, String column) throws SQLException {
        try (ResultSet r = c.getMetaData().getColumns(null, "PUBLIC", table, column)) { return r.next(); }
    }
    private static void index(Connection c, String name, String table, String columns) throws SQLException {
        try (ResultSet r = c.getMetaData().getIndexInfo(null, "PUBLIC", table.toUpperCase(java.util.Locale.ROOT), false, false)) {
            while (r.next()) if (name.equalsIgnoreCase(r.getString("INDEX_NAME"))) return;
        }
        execute(c, "CREATE INDEX " + name + " ON " + table + "(" + columns + ")");
    }
    private static void requireZero(Connection c, String sql, String description) throws SQLException {
        try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(sql)) {
            r.next(); if (r.getLong(1) != 0) throw new SQLException("AT migration validation failed: " + description);
        }
    }
}
