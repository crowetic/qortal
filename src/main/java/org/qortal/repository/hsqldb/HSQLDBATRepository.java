package org.qortal.repository.hsqldb;

import com.google.common.primitives.Longs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.qortal.crypto.Crypto;
import org.qortal.data.at.ATData;
import org.qortal.data.at.ATStateData;
import org.qortal.repository.ATRepository;
import org.qortal.repository.DataException;
import org.qortal.utils.ByteArray;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.qortal.data.account.AccountData;

public class HSQLDBATRepository implements ATRepository {

	private static final Logger LOGGER = LogManager.getLogger(HSQLDBATRepository.class);
	private static final int PAYLOAD_CLEANUP_SWEEP_HEIGHTS = 10_000;
	private static final int MAX_PAYLOAD_BUNDLES_PER_TRIM = 512;

	protected HSQLDBRepository repository;
	private final HSQLDBATStore store;

	public HSQLDBATRepository(HSQLDBRepository repository) {
		this.repository = repository;
		this.store = new HSQLDBATStore(repository);
	}

	// ATs

	@Override
	public ATData fromATAddress(String atAddress) throws DataException {
		String sql = "SELECT ATs.creator,ATs.created_when,ATs.version,ATs.asset_id,ATCode.code_bytes,ATs.code_hash, "
				+ "ATCurrentState.is_sleeping,ATCurrentState.sleep_until_height,ATCurrentState.is_finished,ATCurrentState.had_fatal_error, "
				+ "ATCurrentState.is_frozen,ATCurrentState.frozen_balance,ATCurrentState.sleep_until_message_timestamp "
				+ "FROM ATDefinitions ATs JOIN ATCurrentState USING(AT_address) JOIN ATCode USING(code_hash) "
				+ "WHERE ATs.AT_address = ? LIMIT 1";

		try (ResultSet resultSet = this.repository.checkedExecute(sql, atAddress)) {
			if (resultSet == null)
				return null;

			byte[] creatorPublicKey = resultSet.getBytes(1);
			long created = resultSet.getLong(2);
			int version = resultSet.getInt(3);
			long assetId = resultSet.getLong(4);
			byte[] codeBytes = resultSet.getBytes(5); // Actually BLOB
			byte[] codeHash = resultSet.getBytes(6);
			boolean isSleeping = resultSet.getBoolean(7);

			Integer sleepUntilHeight = resultSet.getInt(8);
			if (sleepUntilHeight == 0 && resultSet.wasNull())
				sleepUntilHeight = null;

			boolean isFinished = resultSet.getBoolean(9);
			boolean hadFatalError = resultSet.getBoolean(10);
			boolean isFrozen = resultSet.getBoolean(11);

			Long frozenBalance = resultSet.getLong(12);
			if (frozenBalance == 0 && resultSet.wasNull())
				frozenBalance = null;

			Long sleepUntilMessageTimestamp = resultSet.getLong(13);
			if (sleepUntilMessageTimestamp == 0 && resultSet.wasNull())
				sleepUntilMessageTimestamp = null;

			return new ATData(atAddress, creatorPublicKey, created, version, assetId, codeBytes, codeHash,
					isSleeping, sleepUntilHeight, isFinished, hadFatalError, isFrozen, frozenBalance,
					sleepUntilMessageTimestamp);
		} catch (SQLException e) {
			throw new DataException("Unable to fetch AT from repository", e);
		}
	}

	@Override
	public List<ATData> fromATAddresses(List<String> atAddresses) throws DataException {
		String sql = "SELECT ATs.creator,ATs.created_when,ATs.version,ATs.asset_id,ATCode.code_bytes,ATs.code_hash, "
				+ "ATCurrentState.is_sleeping,ATCurrentState.sleep_until_height,ATCurrentState.is_finished,ATCurrentState.had_fatal_error, "
				+ "ATCurrentState.is_frozen,ATCurrentState.frozen_balance,ATCurrentState.sleep_until_message_timestamp,ATs.AT_address "
				+ "FROM ATDefinitions ATs JOIN ATCurrentState USING(AT_address) JOIN ATCode USING(code_hash) "
				+ "WHERE ATs.AT_address IN ("
				+ String.join(", ", Collections.nCopies(atAddresses.size(), "?"))
				+ ")"
				;

		List<ATData> list;
		try (ResultSet resultSet = this.repository.checkedExecute(sql, atAddresses.toArray(new String[atAddresses.size()]))) {
			if (resultSet == null) {
				return new ArrayList<>(0);
			}

			list = new ArrayList<>(atAddresses.size());

			do {
				byte[] creatorPublicKey = resultSet.getBytes(1);
				long created = resultSet.getLong(2);
				int version = resultSet.getInt(3);
				long assetId = resultSet.getLong(4);
				byte[] codeBytes = resultSet.getBytes(5); // Actually BLOB
				byte[] codeHash = resultSet.getBytes(6);
				boolean isSleeping = resultSet.getBoolean(7);

				Integer sleepUntilHeight = resultSet.getInt(8);
				if (sleepUntilHeight == 0 && resultSet.wasNull())
					sleepUntilHeight = null;

				boolean isFinished = resultSet.getBoolean(9);
				boolean hadFatalError = resultSet.getBoolean(10);
				boolean isFrozen = resultSet.getBoolean(11);

				Long frozenBalance = resultSet.getLong(12);
				if (frozenBalance == 0 && resultSet.wasNull())
					frozenBalance = null;

				Long sleepUntilMessageTimestamp = resultSet.getLong(13);
				if (sleepUntilMessageTimestamp == 0 && resultSet.wasNull())
					sleepUntilMessageTimestamp = null;

				String atAddress = resultSet.getString(14);

				list.add(new ATData(atAddress, creatorPublicKey, created, version, assetId, codeBytes, codeHash,
						isSleeping, sleepUntilHeight, isFinished, hadFatalError, isFrozen, frozenBalance,
						sleepUntilMessageTimestamp));
			} while ( resultSet.next());

			return list;
		} catch (SQLException e) {
			throw new DataException("Unable to fetch AT from repository", e);
		}
	}

	@Override
	public boolean exists(String atAddress) throws DataException {
		try {
			return this.repository.exists("ATDefinitions", "AT_address = ?", atAddress);
		} catch (SQLException e) {
			throw new DataException("Unable to check for AT in repository", e);
		}
	}

	@Override
	public byte[] getCreatorPublicKey(String atAddress) throws DataException {
		String sql = "SELECT creator FROM ATDefinitions WHERE AT_address = ?";

		try (ResultSet resultSet = this.repository.checkedExecute(sql, atAddress)) {
			if (resultSet == null)
				return null;

			return resultSet.getBytes(1);
		} catch (SQLException e) {
			throw new DataException("Unable to fetch AT creator's public key from repository", e);
		}
	}

	@Override
	public List<ATData> getAllExecutableATs() throws DataException {
		String sql = "SELECT ATs.AT_address,ATs.creator,ATs.created_when,ATs.version,ATs.asset_id,ATCode.code_bytes,ATs.code_hash, "
				+ "ATCurrentState.is_sleeping,ATCurrentState.sleep_until_height,ATCurrentState.had_fatal_error, "
				+ "ATCurrentState.is_frozen,ATCurrentState.frozen_balance,ATCurrentState.sleep_until_message_timestamp "
				+ "FROM ATDefinitions ATs JOIN ATCurrentState USING(AT_address) JOIN ATCode USING(code_hash) "
				+ "WHERE ATCurrentState.is_finished = false "
				+ "ORDER BY ATs.created_when ASC,ATs.AT_address DESC";

		List<ATData> executableATs = new ArrayList<>();

		try (ResultSet resultSet = this.repository.checkedExecute(sql)) {
			if (resultSet == null)
				return executableATs;

			boolean isFinished = false;

			do {
				String atAddress = resultSet.getString(1);
				byte[] creatorPublicKey = resultSet.getBytes(2);
				long created = resultSet.getLong(3);
				int version = resultSet.getInt(4);
				long assetId = resultSet.getLong(5);
				byte[] codeBytes = resultSet.getBytes(6); // Actually BLOB
				byte[] codeHash = resultSet.getBytes(7);
				boolean isSleeping = resultSet.getBoolean(8);

				Integer sleepUntilHeight = resultSet.getInt(9);
				if (sleepUntilHeight == 0 && resultSet.wasNull())
					sleepUntilHeight = null;

				boolean hadFatalError = resultSet.getBoolean(10);
				boolean isFrozen = resultSet.getBoolean(11);

				Long frozenBalance = resultSet.getLong(12);
				if (frozenBalance == 0 && resultSet.wasNull())
					frozenBalance = null;

				Long sleepUntilMessageTimestamp = resultSet.getLong(13);
				if (sleepUntilMessageTimestamp == 0 && resultSet.wasNull())
					sleepUntilMessageTimestamp = null;

				ATData atData = new ATData(atAddress, creatorPublicKey, created, version, assetId, codeBytes, codeHash,
						isSleeping, sleepUntilHeight, isFinished, hadFatalError, isFrozen, frozenBalance,
						sleepUntilMessageTimestamp);

				executableATs.add(atData);
			} while (resultSet.next());

			return executableATs;
		} catch (SQLException e) {
			throw new DataException("Unable to fetch executable ATs from repository", e);
		}
	}

    @Override
    public List<ATData> getATsForExecution(int candidateHeight) throws DataException {
        String sql = "SELECT d.AT_address,d.creator,d.created_when,d.version,d.asset_id,d.code_hash,c.is_sleeping,c.sleep_until_height,"
                + "c.is_finished,c.had_fatal_error,c.is_frozen,c.frozen_balance,c.sleep_until_message_timestamp "
                + "FROM (SELECT AT_address FROM ATCurrentState WHERE schedule_class=0 "
                + "UNION SELECT AT_address FROM ATCurrentState WHERE schedule_class=1 AND message_pending=TRUE "
                + "UNION SELECT AT_address FROM ATCurrentState WHERE schedule_class=1 AND sleep_until_height<>0 AND sleep_until_height<=?) ready "
                + "JOIN ATDefinitions d ON d.AT_address=ready.AT_address JOIN ATCurrentState c ON c.AT_address=d.AT_address "
                + "ORDER BY d.created_when ASC,d.AT_address DESC";
        List<ATData> result = new ArrayList<>();
        try (ResultSet rs = repository.checkedExecute(sql, candidateHeight)) {
            if (rs != null) do {
                result.add(new ATData(rs.getString(1),rs.getBytes(2),rs.getLong(3),rs.getInt(4),rs.getLong(5),null,rs.getBytes(6),
                        rs.getBoolean(7),(Integer)rs.getObject(8),rs.getBoolean(9),rs.getBoolean(10),rs.getBoolean(11),
                        (Long)rs.getObject(12),(Long)rs.getObject(13)));
            } while (rs.next());
        } catch (SQLException e) { throw new DataException("Unable to select scheduled ATs",e); }
        return result;
    }

	@Override
	public List<ATData> getATsByFunctionality(byte[] codeHash, Boolean isExecutable, Integer limit, Integer offset, Boolean reverse) throws DataException {
		StringBuilder sql = new StringBuilder(512);
		List<Object> bindParams = new ArrayList<>();

		sql.append("SELECT ATs.AT_address,ATs.creator,ATs.created_when,ATs.version,ATs.asset_id,ATCode.code_bytes, ")
				.append("ATCurrentState.is_sleeping,ATCurrentState.sleep_until_height,ATCurrentState.is_finished,ATCurrentState.had_fatal_error, ")
				.append("ATCurrentState.is_frozen,ATCurrentState.frozen_balance,ATCurrentState.sleep_until_message_timestamp ")
				.append("FROM ATDefinitions ATs JOIN ATCurrentState USING(AT_address) JOIN ATCode USING(code_hash) ")
				.append("WHERE code_hash = ? ");
		bindParams.add(codeHash);

		if (isExecutable != null) {
			sql.append("AND ATCurrentState.is_finished != ? ");
			bindParams.add(isExecutable);
		}

		sql.append("ORDER BY created_when ");
		if (reverse != null && reverse)
			sql.append("DESC");

		HSQLDBRepository.limitOffsetSql(sql, limit, offset);

		List<ATData> matchingATs = new ArrayList<>();

		try (ResultSet resultSet = this.repository.checkedExecute(sql.toString(), bindParams.toArray())) {
			if (resultSet == null)
				return matchingATs;

			do {
				String atAddress = resultSet.getString(1);
				byte[] creatorPublicKey = resultSet.getBytes(2);
				long created = resultSet.getLong(3);
				int version = resultSet.getInt(4);
				long assetId = resultSet.getLong(5);
				byte[] codeBytes = resultSet.getBytes(6); // Actually BLOB
				boolean isSleeping = resultSet.getBoolean(7);

				Integer sleepUntilHeight = resultSet.getInt(8);
				if (sleepUntilHeight == 0 && resultSet.wasNull())
					sleepUntilHeight = null;

				boolean isFinished = resultSet.getBoolean(9);

				boolean hadFatalError = resultSet.getBoolean(10);
				boolean isFrozen = resultSet.getBoolean(11);

				Long frozenBalance = resultSet.getLong(12);
				if (frozenBalance == 0 && resultSet.wasNull())
					frozenBalance = null;

				Long sleepUntilMessageTimestamp = resultSet.getLong(13);
				if (sleepUntilMessageTimestamp == 0 && resultSet.wasNull())
					sleepUntilMessageTimestamp = null;

				ATData atData = new ATData(atAddress, creatorPublicKey, created, version, assetId, codeBytes, codeHash,
						isSleeping, sleepUntilHeight, isFinished, hadFatalError, isFrozen, frozenBalance,
						sleepUntilMessageTimestamp);

				matchingATs.add(atData);
			} while (resultSet.next());

			return matchingATs;
		} catch (SQLException e) {
			throw new DataException("Unable to fetch matching ATs from repository", e);
		}
	}

	@Override
	public List<ATData> getAllATsByFunctionality(Set<ByteArray> codeHashes, Boolean isExecutable) throws DataException {
		return getAllATsByFunctionality(codeHashes, isExecutable, null, true);
	}

	@Override
	public List<ATData> getAllATsByFunctionalitySince(Set<ByteArray> codeHashes, Boolean isExecutable,
			Integer minimumLatestStateHeight) throws DataException {
		return getAllATsByFunctionality(codeHashes, isExecutable, minimumLatestStateHeight, false);
	}

	private List<ATData> getAllATsByFunctionality(Set<ByteArray> codeHashes, Boolean isExecutable,
			Integer minimumLatestStateHeight, boolean includeCodeBytes) throws DataException {
		if (codeHashes == null || codeHashes.isEmpty())
			return new ArrayList<>();

		StringBuilder sql = new StringBuilder(512);
		List<Object> bindParams = new ArrayList<>();

		sql.append("SELECT AT_address, creator, created_when, version, asset_id, ")
				.append(includeCodeBytes ? "code_bytes, " : "CAST(NULL AS BLOB), ")
				.append("is_sleeping, sleep_until_height, is_finished, had_fatal_error, ")
				.append("is_frozen, frozen_balance, code_hash, sleep_until_message_timestamp ")
				.append("FROM ");

		// (VALUES (?), (?), ...) AS ATCodeHashes (code_hash)
		sql.append("(VALUES ");

		boolean isFirst = true;
		for (ByteArray codeHash : codeHashes) {
			if (!isFirst)
				sql.append(", ");
			else
				isFirst = false;

			sql.append("(CAST(? AS VARBINARY(256)))");
			bindParams.add(codeHash.value);
		}
		sql.append(") AS ATCodeHashes (code_hash) ");

		sql.append("JOIN ATDefinitions ATs USING(code_hash) JOIN ATCurrentState USING(AT_address) JOIN ATCode USING(code_hash) ");

		boolean hasWhere = false;
		if (isExecutable != null) {
			sql.append("WHERE ATCurrentState.is_finished != ? ");
			bindParams.add(isExecutable);
			hasWhere = true;
		}

		if (minimumLatestStateHeight != null) {
			sql.append(hasWhere ? "AND " : "WHERE ");
			sql.append("ATCurrentState.latest_state_height >= ? ");
			bindParams.add(minimumLatestStateHeight);
		}

		List<ATData> matchingATs = new ArrayList<>();

		try (ResultSet resultSet = this.repository.checkedExecute(sql.toString(), bindParams.toArray())) {
			if (resultSet == null)
				return matchingATs;

			do {
				String atAddress = resultSet.getString(1);
				byte[] creatorPublicKey = resultSet.getBytes(2);
				long created = resultSet.getLong(3);
				int version = resultSet.getInt(4);
				long assetId = resultSet.getLong(5);
				byte[] codeBytes = resultSet.getBytes(6); // Actually BLOB
				boolean isSleeping = resultSet.getBoolean(7);

				Integer sleepUntilHeight = resultSet.getInt(8);
				if (sleepUntilHeight == 0 && resultSet.wasNull())
					sleepUntilHeight = null;

				boolean isFinished = resultSet.getBoolean(9);

				boolean hadFatalError = resultSet.getBoolean(10);
				boolean isFrozen = resultSet.getBoolean(11);

				Long frozenBalance = resultSet.getLong(12);
				if (frozenBalance == 0 && resultSet.wasNull())
					frozenBalance = null;

				byte[] codeHash = resultSet.getBytes(13);
				Long sleepUntilMessageTimestamp = resultSet.getLong(14);

				ATData atData = new ATData(atAddress, creatorPublicKey, created, version, assetId, codeBytes, codeHash,
						isSleeping, sleepUntilHeight, isFinished, hadFatalError, isFrozen, frozenBalance, sleepUntilMessageTimestamp);

				matchingATs.add(atData);
			} while (resultSet.next());

			return matchingATs;
		} catch (SQLException e) {
			throw new DataException("Unable to fetch matching ATs from repository", e);
		}
	}

	@Override
	public Integer getATCreationBlockHeight(String atAddress) throws DataException {
		String sql = "SELECT block_height "
				+ "FROM DeployATTransactions "
				+ "JOIN Transactions USING (signature) "
				+ "WHERE AT_address = ? "
				+ "LIMIT 1";

		try (ResultSet resultSet = this.repository.checkedExecute(sql, atAddress)) {
			if (resultSet == null)
				return null;

			return resultSet.getInt(1);
		} catch (SQLException e) {
			throw new DataException("Unable to fetch AT's creation block height from repository", e);
		}
	}

	@Override
	public void save(ATData atData) throws DataException {
        try { store.saveDefinition(atData); }
        catch (SQLException e) { throw new DataException("Unable to save AT", e); }
    }

    @Override
    public void saveBlockResult(int height, byte[] blob, List<ATData> updatedHeads, List<ATStateData> states) throws DataException {
        try { store.saveBlockResult(height, blob, updatedHeads, states); }
        catch (SQLException e) { throw new DataException("Unable to save AT block result", e); }
    }

    @Override
    public byte[] getBlockResult(int height) throws DataException {
        try { return store.readBlockResult(height); }
        catch (SQLException e) { throw new DataException("Unable to read AT block result", e); }
    }

    @Override
    public void deleteBlockResult(int height) throws DataException {
        try { store.deleteBlockResult(height); }
        catch (SQLException e) { throw new DataException("Unable to delete AT block result", e); }
    }

    @Override
    public void saveATMetadata(List<ATData> ats) throws DataException {
        try { store.saveHeads(ats); }
        catch (SQLException e) { throw new DataException("Unable to restore AT metadata", e); }
    }

	@Override
	public void delete(String atAddress) throws DataException {
		try {
			this.repository.delete("ATDefinitions", "AT_address = ?", atAddress);
			// AT States also deleted via ON DELETE CASCADE
		} catch (SQLException e) {
			throw new DataException("Unable to delete AT from repository", e);
		}
	}

	@Override
	public void setCurrentStateHead(String atAddress, int height, byte[] payloadHash) throws DataException {
		try {
			repository.executeCheckedUpdate("UPDATE ATCurrentState SET latest_state_height=?,payload_hash=? WHERE AT_address=?",
				height, payloadHash, atAddress);
		} catch (SQLException e) {
			throw new DataException("Unable to set AT current state head", e);
		}
	}

	// AT State

	@Override
	public ATStateData getATStateAtHeight(String atAddress, int height) throws DataException {
		try {
			byte[] blob = store.readBlockResult(height);
			if (blob != null) {
				for (org.qortal.at.ATBlockResult.Entry e : org.qortal.at.ATBlockResult.deserialize(blob)) {
					if (e.address.equals(atAddress)) {
						byte[] data = store.stateIfPresent(atAddress, height, e.stateHash);
						return new ATStateData(atAddress, height, data, e.stateHash, e.fees, e.isInitial, e.sleepUntilMessageTimestamp);
					}
				}
			}

			try (ResultSet rs = repository.checkedExecute("SELECT s.state_hash,s.fees,s.is_initial,s.sleep_until_message_timestamp "
					+ "FROM ATStates s "
					+ "WHERE s.AT_address=? AND s.height=?", atAddress, height)) {
				if (rs == null) return null;
				byte[] hash = rs.getBytes(1);
				return new ATStateData(atAddress, height, store.stateIfPresent(atAddress, height, hash), hash,
						rs.getLong(2), rs.getBoolean(3), (Long) rs.getObject(4));
			}
		} catch (SQLException e) {
			throw new DataException("Unable to fetch AT state from repository", e);
		}
	}

    @Override
    public ATStateData getLatestATState(String atAddress) throws DataException {
        List<ATStateData> states = getLatestATStates(Collections.singletonList(atAddress));
        return states.isEmpty() ? null : states.get(0);
    }

    @Override
    public byte[] getCodePayload(byte[] hash) throws DataException {
        try { return store.code(hash); }
        catch (SQLException e) { throw new DataException("Unable to load AT code", e); }
    }

    @Override
    public byte[] getStatePayload(byte[] hash) throws DataException {
        try { return store.state(hash); }
        catch (SQLException e) { throw new DataException("Unable to load AT payload", e); }
    }

	@Override
	public byte[] getStatePayload(String address, int height, byte[] hash) throws DataException {
		try { return store.state(address, height, hash); }
		catch (SQLException e) { throw new DataException("Unable to load AT payload", e); }
	}

    @Override
    public List<org.qortal.data.at.ATExecutionState> getExecutionStates(List<String> addresses) throws DataException {
        List<org.qortal.data.at.ATExecutionState> result = new ArrayList<>();
        for (int start = 0; start < addresses.size(); start += 256) {
            List<String> chunk = addresses.subList(start, Math.min(start + 256, addresses.size()));
            String sql = "SELECT c.AT_address,c.latest_state_height,c.payload_hash,COALESCE(b.balance,0) FROM ATCurrentState c "
                    + "LEFT JOIN AccountBalances b ON b.account=c.AT_address AND b.asset_id=0 WHERE c.AT_address IN ("
                    + String.join(",", Collections.nCopies(chunk.size(), "?")) + ")";
            try (ResultSet rs = repository.checkedExecute(sql, chunk.toArray())) {
                if (rs != null) do {
                    byte[] hash = rs.getBytes(3);
                    int height = rs.getInt(2);
                    if (rs.wasNull() || hash == null) throw new DataException("Missing AT execution head");
                    result.add(new org.qortal.data.at.ATExecutionState(rs.getString(1), height, hash, rs.getLong(4)));
                } while (rs.next());
            } catch (SQLException e) { throw new DataException("Unable to load execution heads", e); }
        }
        if (result.size() != addresses.size()) throw new DataException("Missing AT execution heads");
        return result;
    }

    @Override
    public List<ATStateData> getLatestATStates(List<String> addresses) throws DataException {
        List<ATStateData> result = new ArrayList<>();
        for (int start = 0; start < addresses.size(); start += 256) {
            List<String> chunk = addresses.subList(start, Math.min(start + 256, addresses.size()));
            String sql = "SELECT c.AT_address,c.latest_state_height,c.payload_hash,c.sleep_until_message_timestamp "
                    + "FROM ATCurrentState c WHERE c.AT_address IN ("
                    + String.join(",", Collections.nCopies(chunk.size(), "?")) + ")";
            List<ATStateData> metadata = new ArrayList<>();
            try (ResultSet rs = repository.checkedExecute(sql, chunk.toArray())) {
                if (rs != null) do {
                    int height = rs.getInt(2);
                    byte[] payloadHash = rs.getBytes(3);
                    if (rs.wasNull() || payloadHash == null) continue; // Deployed but not yet executed
                    Long cursor = (Long) rs.getObject(4);
                    metadata.add(new ATStateData(rs.getString(1), height, payloadHash, payloadHash, 0L, false, cursor));
                } while (rs.next());
            } catch (SQLException e) { throw new DataException("Unable to load AT heads", e); }
            try { store.preloadStates(metadata); }
            catch (SQLException e) { throw new DataException("Unable to bulk load AT payloads", e); }
            for (ATStateData m : metadata) {
                try { result.add(new ATStateData(m.getATAddress(), m.getHeight(), store.state(m.getATAddress(), m.getHeight(), m.getStateHash()), m.getStateHash(), 0L, false, m.getSleepUntilMessageTimestamp())); }
                catch (SQLException e) { throw new DataException("Unable to load AT payload", e); }
            }
        }
        return result;
    }

	@Override
	public List<ATStateData> getMatchingFinalATStates(byte[] codeHash, byte[] buyerPublicKey, byte[] sellerPublicKey, Boolean isFinished,
			  Integer dataByteOffset, Long expectedValue, Integer minimumFinalHeight,
			  Integer limit, Integer offset, Boolean reverse) throws DataException {
		StringBuilder sql = new StringBuilder(1024);
		List<Object> bindParams = new ArrayList<>();

		sql.append("SELECT ATs.AT_address, c.latest_state_height, c.payload_hash, "
				+ "c.sleep_until_message_timestamp "
				+ "FROM ATDefinitions ATs "
				+ "JOIN ATCurrentState c ON c.AT_address = ATs.AT_address "
				+ "WHERE ATs.code_hash = ? ");
		bindParams.add(codeHash);

		if (isFinished != null) {
			sql.append("AND c.is_finished = ? ");
			bindParams.add(isFinished);
		}

		if (buyerPublicKey != null && buyerPublicKey.length > 0) {
			sql.append("AND EXISTS(SELECT 1 FROM ATTransactions tx WHERE tx.at_address=ATs.AT_address AND tx.recipient=?) ");
			sql.append("AND ATs.creator != ? ");
			bindParams.add(Crypto.toAddress(buyerPublicKey));
			bindParams.add(buyerPublicKey);
		}

		if (sellerPublicKey != null && sellerPublicKey.length > 0) {
			sql.append("AND ATs.creator = ? ");
			bindParams.add(sellerPublicKey);
		}
		if (minimumFinalHeight != null) {
			sql.append("AND c.latest_state_height >= ? ");
			bindParams.add(minimumFinalHeight);
		}

		sql.append(" ORDER BY c.latest_state_height ");
		if (reverse != null && reverse)
			sql.append("DESC");

		boolean filterPayload = dataByteOffset != null && expectedValue != null;
		if (!filterPayload) HSQLDBRepository.limitOffsetSql(sql, limit, offset);

		List<ATStateData> metadata = new ArrayList<>();

		try (ResultSet resultSet = this.repository.checkedExecute(sql.toString(), bindParams.toArray())) {
			if (resultSet == null)
				return metadata;

			do {
				String atAddress = resultSet.getString(1);
				int height = resultSet.getInt(2);
				byte[] stateHash = resultSet.getBytes(3);

				Long sleepUntilMessageTimestamp = resultSet.getLong(4);
				if (sleepUntilMessageTimestamp == 0 && resultSet.wasNull())
					sleepUntilMessageTimestamp = null;
				metadata.add(new ATStateData(atAddress, height, stateHash, stateHash, 0L, false, sleepUntilMessageTimestamp));
			} while (resultSet.next());
		} catch (SQLException e) {
			throw new DataException("Unable to fetch matching AT states from repository", e);
		}

		try { store.preloadStates(metadata); }
		catch (SQLException e) { throw new DataException("Unable to bulk load matching AT payloads", e); }
		List<ATStateData> result = new ArrayList<>();
		int skipped = 0;
		byte[] expectedBytes = filterPayload ? Longs.toByteArray(expectedValue) : null;
		for (ATStateData item : metadata) {
			byte[] data;
			try { data = store.state(item.getATAddress(), item.getHeight(), item.getStateHash()); }
			catch (SQLException e) { throw new DataException("Unable to load matching AT payload", e); }
			if (filterPayload && (dataByteOffset < 0 || dataByteOffset + expectedBytes.length > data.length
					|| !java.util.Arrays.equals(expectedBytes, java.util.Arrays.copyOfRange(data, dataByteOffset, dataByteOffset + expectedBytes.length))))
				continue;
			if (filterPayload && skipped++ < (offset == null ? 0 : offset)) continue;
			result.add(new ATStateData(item.getATAddress(), item.getHeight(), data, item.getStateHash(), 0L, false,
					item.getSleepUntilMessageTimestamp()));
			if (filterPayload && limit != null && result.size() >= limit) break;
		}
		return result;
	}

	@Override
	public List<ATStateData> getMatchingFinalATStatesQuorum(byte[] codeHash, Boolean isFinished,
			Integer dataByteOffset, Long expectedValue,
			int minimumCount, int maximumCount, long minimumPeriod) throws DataException {
		// We need most recent entry first so we can use its timestamp to slice further results
		List<ATStateData> mostRecentStates = this.getMatchingFinalATStates(codeHash, null, null, isFinished,
				dataByteOffset, expectedValue, null,
				1, 0, true);

		if (mostRecentStates == null)
			return null;

		if (mostRecentStates.isEmpty())
			return mostRecentStates;

		// Slice so that we meet both minimumCount and minimumPeriod
		int minimumHeight = mostRecentStates.get(0).getHeight() - (int) (minimumPeriod / 60 * 1000L);
		List<ATStateData> candidates = this.getMatchingFinalATStates(codeHash, null, null, isFinished,
				dataByteOffset, expectedValue, null, null, null, true);
		List<ATStateData> result = new ArrayList<>();
		for (ATStateData state : candidates) {
			if (state.getHeight() < minimumHeight && result.size() >= minimumCount) break;
			if (result.size() >= maximumCount) break;
			result.add(state);
		}
		return result;
	}

	@Override
	public List<ATStateData> getBlockATStatesAtHeight(int height) throws DataException {
		List<ATStateData> atStates = new ArrayList<>();
		Set<String> addresses = new HashSet<>();
		try {
			byte[] blob = store.readBlockResult(height);
			if (blob != null) {
				List<org.qortal.at.ATBlockResult.Entry> entries = org.qortal.at.ATBlockResult.deserialize(blob);
				for (org.qortal.at.ATBlockResult.Entry e : entries) {
					ATStateData state = new ATStateData(e.address, height, e.stateHash, e.fees, e.isInitial);
					atStates.add(state);
					addresses.add(e.address);
				}
			}
		} catch (SQLException e) {
			throw new DataException("Unable to fetch AT block result", e);
		}

		// Pre-migration history and initial deployments remain as compact metadata rows.
		try (ResultSet resultSet = this.repository.checkedExecute(
				"SELECT s.AT_address,s.state_hash,s.fees,s.is_initial FROM ATStates s "
						+ "JOIN ATDefinitions d ON d.AT_address=s.AT_address WHERE s.height=? "
						+ "ORDER BY d.created_when ASC,d.AT_address DESC", height)) {
			if (resultSet != null) do {
				String address = resultSet.getString(1);
				if (addresses.add(address))
					atStates.add(new ATStateData(address, height,
							resultSet.getBytes(2), resultSet.getLong(3), resultSet.getBoolean(4)));
			} while (resultSet.next());
			return atStates;
		} catch (SQLException e) {
			throw new DataException("Unable to fetch AT states for this height from repository", e);
		}
	}


	@Override
	public int getAtTrimHeight() throws DataException {
		String sql = "SELECT AT_trim_height FROM DatabaseInfo";

		try (ResultSet resultSet = this.repository.checkedExecute(sql)) {
			if (resultSet == null)
				return 0;

			return resultSet.getInt(1);
		} catch (SQLException e) {
			throw new DataException("Unable to fetch AT state trim height from repository", e);
		}
	}

	@Override
	public void setAtTrimHeight(int trimHeight) throws DataException {
		// trimHeightsLock is to prevent concurrent update on DatabaseInfo
		// that could result in "transaction rollback: serialization failure"
		synchronized (this.repository.trimHeightsLock) {
			String updateSql = "UPDATE DatabaseInfo SET AT_trim_height = ?";

			try {
				this.repository.executeCheckedUpdate(updateSql, trimHeight);
				this.repository.saveChanges();
			} catch (SQLException e) {
				this.repository.examineException(e);
				throw new DataException("Unable to set AT state trim height in repository", e);
			}
		}
	}

	@Override
	public int trimAtStates(int minHeight, int maxHeight, int limit) throws DataException {
		return this.trimAtStatesDetailed(minHeight, maxHeight, limit).getTrimmed();
	}

	@Override
	public ATTrimResult trimAtStatesDetailed(int minHeight, int maxHeight, int limit) throws DataException {
		if (minHeight <= 1) minHeight = 2;
		limit = Math.min(limit, MAX_PAYLOAD_BUNDLES_PER_TRIM);
		if (limit <= 0) return new ATTrimResult(0, minHeight > maxHeight);
		try {
			// Move each AT's rollback boundary forward before clearing a bundle. This is the
			// predecessor needed when an AT sleeps across the trim frontier.
			Map<Integer, byte[]> candidates = new LinkedHashMap<>();
			boolean rangeComplete = true;
			if (minHeight <= maxHeight) {
				int nextCurrentHeight = loadTrimCandidates(candidates, minHeight, maxHeight, limit);
				rangeComplete = nextCurrentHeight > maxHeight;
			}

			// Once the current range is covered, spend any remaining bounded work on the
			// older cleanup cursor. This also runs when the main trim cursor is caught up.
			if (rangeComplete && candidates.size() < limit && minHeight > 2) {
				int cleanupHeight = getPayloadCleanupHeight();
				if (cleanupHeight < 2) cleanupHeight = 2;
				if (cleanupHeight < minHeight) {
					int cleanupEnd = Math.min(minHeight - 1, cleanupHeight + PAYLOAD_CLEANUP_SWEEP_HEIGHTS - 1);
					int nextCleanupHeight = loadTrimCandidates(candidates, cleanupHeight, cleanupEnd, limit);
					if (nextCleanupHeight > cleanupEnd) nextCleanupHeight = cleanupEnd + 1;
					repository.executeCheckedUpdate("UPDATE ATStoreInfo SET cleanup_height=? WHERE id=1",
							Math.min(nextCleanupHeight, minHeight));
				}
			}
			if (candidates.isEmpty()) return new ATTrimResult(0, rangeComplete);

			Map<String, BoundaryPointer> newest = new HashMap<>();
			for (Map.Entry<Integer, byte[]> candidate : candidates.entrySet()) {
				for (Map.Entry<String, org.qortal.at.ATStateBundle.Entry> entry
						: org.qortal.at.ATStateBundle.deserialize(candidate.getValue()).entrySet()) {
					BoundaryPointer prior = newest.get(entry.getKey());
					if (prior == null || candidate.getKey() > prior.height)
						newest.put(entry.getKey(), new BoundaryPointer(candidate.getKey(), entry.getValue().hash));
				}
			}

			List<String> addresses = new ArrayList<>(newest.keySet());
			List<Object[]> updates = newest.entrySet().stream()
					.map(entry -> new Object[]{entry.getValue().height, entry.getValue().hash, entry.getKey(), entry.getValue().height})
					.collect(Collectors.toList());
			repository.executeCheckedBatchUpdate(
					"UPDATE ATCurrentState SET rollback_state_height=?,rollback_payload_hash=? "
							+ "WHERE AT_address=? AND (rollback_state_height IS NULL OR rollback_state_height<?)", updates);

			// A pointer can outlive its block bundle (finished and long-sleeping ATs are
			// common). Detach exactly those entries before deleting every expired bundle.
			// Both actions are part of this repository transaction, so a crash exposes
			// either the complete bundle or the verified retained row, never neither.
			Set<String> affectedAddresses = new HashSet<>(addresses);
			affectedAddresses.addAll(store.retainReferencedPayloads(candidates));

			int trimmed = 0;
			List<Integer> heights = new ArrayList<>(candidates.keySet());
			for (int start = 0; start < heights.size(); start += 256) {
				List<Integer> chunk = heights.subList(start, Math.min(start + 256, heights.size()));
				trimmed += repository.executeCheckedUpdate(
						"DELETE FROM ATBlockPayloads b WHERE b.block_height IN ("
								+ String.join(",", Collections.nCopies(chunk.size(), "?")) + ")", chunk.toArray());
			}
			store.deleteUnreferencedRetainedPayloads(affectedAddresses);
			return new ATTrimResult(trimmed, rangeComplete);
		} catch (SQLException | DataException e) { throw new DataException("Unable to trim AT block payload bundles", e); }
	}

	private int loadTrimCandidates(Map<Integer, byte[]> candidates, int minHeight, int maxHeight, int limit)
			throws SQLException {
		int remaining = limit - candidates.size();
		if (remaining <= 0)
			return minHeight;
		int rowCount = 0;
		int lastHeight = minHeight - 1;
		try (ResultSet rows = repository.checkedExecute(
				"SELECT b.block_height,b.payload_bytes FROM ATBlockPayloads b "
						+ "WHERE b.block_height BETWEEN ? AND ? AND b.payload_bytes IS NOT NULL "
						+ "ORDER BY b.block_height LIMIT ?", minHeight, maxHeight, remaining)) {
			if (rows != null) do {
				lastHeight = rows.getInt(1);
				++rowCount;
				candidates.put(lastHeight, rows.getBytes(2));
			} while (rows.next());
		}
		return rowCount < remaining ? maxHeight + 1 : lastHeight + 1;
	}

	private int getPayloadCleanupHeight() throws SQLException {
		try (ResultSet row = repository.checkedExecute("SELECT cleanup_height FROM ATStoreInfo WHERE id=1")) {
			return row == null ? 2 : row.getInt(1);
		}
	}

	private static final class BoundaryPointer {
		private final int height;
		private final byte[] hash;
		private BoundaryPointer(int height, byte[] hash) { this.height = height; this.hash = hash; }
	}


	@Override
	public int getAtPruneHeight() throws DataException {
		String sql = "SELECT AT_prune_height FROM DatabaseInfo";

		try (ResultSet resultSet = this.repository.checkedExecute(sql)) {
			if (resultSet == null)
				return 0;

			return resultSet.getInt(1);
		} catch (SQLException e) {
			throw new DataException("Unable to fetch AT state prune height from repository", e);
		}
	}

	@Override
	public void setAtPruneHeight(int pruneHeight) throws DataException {
		// trimHeightsLock is to prevent concurrent update on DatabaseInfo
		// that could result in "transaction rollback: serialization failure"
		synchronized (this.repository.trimHeightsLock) {
			String updateSql = "UPDATE DatabaseInfo SET AT_prune_height = ?";

			try {
				this.repository.executeCheckedUpdate(updateSql, pruneHeight);
				this.repository.saveChanges();
			} catch (SQLException e) {
				repository.examineException(e);
				throw new DataException("Unable to set AT state prune height in repository", e);
			}
		}
	}

    @Override
    public int pruneAtStates(int minHeight, int maxHeight) throws DataException {
        // Keep the genesis receipt just as pruneBlocks() keeps the genesis block.
        // Apart from preserving the chain boundary, this prevents a fresh node from
        // having a different retained AT-store prefix than an upgraded node.
        if (minHeight <= 1)
            minHeight = 2;
        if (minHeight > maxHeight)
            return 0;
        try {
			Set<String> affectedAddresses;
			Map<Integer, byte[]> payloads = new LinkedHashMap<>();
			try (ResultSet rows = repository.checkedExecute(
					"SELECT block_height,payload_bytes FROM ATBlockPayloads "
							+ "WHERE block_height BETWEEN ? AND ? AND payload_bytes IS NOT NULL ORDER BY block_height",
					minHeight, maxHeight)) {
				if (rows != null) do {
					payloads.put(rows.getInt(1), rows.getBytes(2));
				} while (rows.next());
			}
			affectedAddresses = store.retainReferencedPayloads(payloads);
			int deletedResults = repository.executeCheckedUpdate(
					"DELETE FROM ATBlockResults WHERE block_height BETWEEN ? AND ?",
					minHeight, maxHeight);
			int deletedPayloads = repository.executeCheckedUpdate(
					"DELETE FROM ATBlockPayloads WHERE block_height BETWEEN ? AND ?",
					minHeight, maxHeight);
			// Pre-migration blocks use this compact metadata to locate their parent. Keep
			// metadata for all live pointers even when the corresponding height is old.
			repository.executeCheckedUpdate("DELETE FROM ATStates s WHERE height BETWEEN ? AND ? "
					+ "AND NOT EXISTS(SELECT 1 FROM ATCurrentState c WHERE c.AT_address=s.AT_address AND "
					+ "(c.latest_state_height=s.height OR c.previous_state_height=s.height "
					+ "OR c.rollback_state_height=s.height))", minHeight, maxHeight);
			store.deleteUnreferencedRetainedPayloads(affectedAddresses);
            return Math.max(deletedResults, deletedPayloads);
        } catch (SQLException e) {
            throw new DataException("Unable to prune AT block results", e);
        }
    }

	@Override
	public void saveInitialState(ATStateData atStateData) throws DataException {
		if (!atStateData.isInitial() || atStateData.getStateData() == null)
			throw new IllegalArgumentException("AT deployment state must be initial and contain payload bytes");
        try {
			store.saveInitialState(atStateData);
			saveStateSummary(atStateData);
        } catch (SQLException e) { throw new DataException("Unable to save AT state", e); }
    }

	@Override
	public void saveArchivedStateSummary(ATStateData atStateData) throws DataException {
		if (atStateData.getStateData() != null)
			throw new IllegalArgumentException("Archived AT summary unexpectedly contains payload bytes");
		try { saveStateSummary(atStateData); }
		catch (SQLException e) { throw new DataException("Unable to save archived AT state summary", e); }
	}

	private void saveStateSummary(ATStateData state) throws SQLException {
		new HSQLDBSaver("ATStates")
				.bind("AT_address", state.getATAddress())
				.bind("height", state.getHeight())
				.bind("state_hash", state.getStateHash())
				.bind("fees", state.getFees())
				.bind("is_initial", state.isInitial())
				.bind("sleep_until_message_timestamp", state.getSleepUntilMessageTimestamp())
				.bind("payload_hash", state.getStateHash())
				.execute(repository);
	}

	@Override
	public boolean deleteATStates(int height) throws DataException {
		try { return store.deleteBlockResult(height); }
		catch (SQLException e) { throw new DataException("Unable to delete AT block result", e); }
	}

	// Finding transactions for ATs to process

    public NextTransactionInfo findNextTransaction(String recipient, int height, int sequence) throws DataException {
        // Two bounded seeks preserve NULL sequence ordering in old later-height records.
        String[] predicates = {"block_height = ? AND block_sequence > ?", "block_height > ?"};
        try {
            for (int part = 0; part < 2; ++part) {
                Object[] args = part == 0 ? new Object[]{recipient, height, sequence} : new Object[]{recipient, height};
                try (ResultSet rs = repository.checkedExecute("SELECT block_height,block_sequence,signature FROM ATRecipientTransactions "
                        + "WHERE recipient = ? AND " + predicates[part] + " ORDER BY block_height,block_sequence LIMIT 1", args)) {
                    if (rs != null) return new NextTransactionInfo(rs.getInt(1), rs.getInt(2), rs.getBytes(3));
                }
            }
            return null;
        } catch (SQLException e) { throw new DataException("Unable to seek AT recipient transaction", e); }
    }

	// Other

    /** Block-link boundary: all transactions have executed; wake state is published before the next block executes. */
    public void refreshRecipients(List<org.qortal.data.transaction.TransactionData> transactions, int height) throws DataException {
		List<Object[]> deletions = new ArrayList<>(), candidates = new ArrayList<>(), insertions = new ArrayList<>();
        java.util.Set<String> affected = new java.util.LinkedHashSet<>();
        for (int sequence = 0; sequence < transactions.size(); ++sequence) {
            org.qortal.data.transaction.TransactionData data = transactions.get(sequence);
            String recipient;
            if (data instanceof org.qortal.data.transaction.PaymentTransactionData) recipient = ((org.qortal.data.transaction.PaymentTransactionData)data).getRecipient();
            else if (data instanceof org.qortal.data.transaction.MessageTransactionData) recipient = ((org.qortal.data.transaction.MessageTransactionData)data).getRecipient();
            else if (data instanceof org.qortal.data.transaction.ATTransactionData) recipient = ((org.qortal.data.transaction.ATTransactionData)data).getRecipient();
            else continue;
            deletions.add(new Object[]{data.getSignature()});
            if (recipient != null) {
				candidates.add(new Object[]{data.getSignature(),recipient,height,sequence});
            }
        }
        try {
			java.util.Set<String> ATRecipients = new java.util.HashSet<>();
			java.util.List<String> candidateAddresses = candidates.stream().map(row -> (String) row[1]).distinct()
					.collect(java.util.stream.Collectors.toList());
			for (int start = 0; start < candidateAddresses.size(); start += 256) {
				java.util.List<String> chunk = candidateAddresses.subList(start, Math.min(start + 256, candidateAddresses.size()));
				try (ResultSet rs = repository.checkedExecute("SELECT AT_address FROM ATCurrentState WHERE AT_address IN ("
						+ String.join(",", Collections.nCopies(chunk.size(), "?")) + ")", chunk.toArray())) {
					if (rs != null) do { ATRecipients.add(rs.getString(1)); } while (rs.next());
				}
			}
			for (Object[] row : candidates) {
				String recipient = (String) row[1];
				if (ATRecipients.contains(recipient)) {
					insertions.add(row);
					affected.add(recipient);
				}
			}
            for (int start = 0; start < deletions.size(); start += 256) {
                List<Object[]> chunk = deletions.subList(start,Math.min(start+256,deletions.size()));
                Object[] signatures = chunk.stream().map(row -> row[0]).toArray();
                try (ResultSet rs = repository.checkedExecute("SELECT DISTINCT recipient FROM ATRecipientTransactions WHERE signature IN ("
                        + String.join(",",Collections.nCopies(chunk.size(),"?")) + ")",signatures)) {
                    if (rs != null) do { affected.add(rs.getString(1)); } while (rs.next());
                }
            }
            repository.executeCheckedBatchUpdate("DELETE FROM ATRecipientTransactions WHERE signature=?",deletions);
            repository.executeCheckedBatchUpdate("INSERT INTO ATRecipientTransactions VALUES(?,?,?,?)",insertions);
            List<Object[]> updates = affected.stream().map(address -> new Object[]{address}).collect(java.util.stream.Collectors.toList());
            repository.executeCheckedBatchUpdate("UPDATE ATCurrentState s SET message_pending="
                    + "(EXISTS(SELECT 1 FROM ATRecipientTransactions t WHERE t.recipient=s.AT_address AND t.block_height>s.cursor_height) "
                    + "OR EXISTS(SELECT 1 FROM ATRecipientTransactions t WHERE t.recipient=s.AT_address AND t.block_height=s.cursor_height AND t.block_sequence>s.cursor_sequence)) "
                    + "WHERE s.AT_address=? AND s.schedule_class=1",updates);
        } catch (SQLException e) { throw new DataException("Unable to batch recipient changes",e); }
    }

    /** Called whenever a transaction's confirmed chain position changes. SQL rollback restores the index and schedule together. */
    public void refreshRecipient(byte[] signature) throws DataException {
        try {
            java.util.Set<String> recipients = new java.util.HashSet<>();
            try (ResultSet rs = repository.checkedExecute("SELECT recipient FROM ATRecipientTransactions WHERE signature=?", signature)) {
                if (rs != null) recipients.add(rs.getString(1));
            }
            repository.delete("ATRecipientTransactions", "signature=?", signature);
            String recipient = null; Integer height = null, sequence = null;
            try (ResultSet rs = repository.checkedExecute("SELECT COALESCE(p.recipient,m.recipient,a.recipient),t.block_height,t.block_sequence "
                    + "FROM Transactions t LEFT JOIN PaymentTransactions p USING(signature) LEFT JOIN MessageTransactions m USING(signature) "
                    + "LEFT JOIN ATTransactions a USING(signature) WHERE t.signature=? AND t.block_height IS NOT NULL", signature)) {
                if (rs != null) { recipient = rs.getString(1); height = (Integer)rs.getObject(2); sequence = (Integer)rs.getObject(3); }
            }
			if (recipient != null && repository.exists("ATCurrentState", "AT_address=?", recipient)) {
                repository.executeCheckedUpdate("INSERT INTO ATRecipientTransactions VALUES(?,?,?,?)", signature, recipient, height, sequence);
                recipients.add(recipient);
            }
            for (String address : recipients)
                repository.executeCheckedUpdate("UPDATE ATCurrentState s SET message_pending=EXISTS "
                        + "(SELECT 1 FROM ATRecipientTransactions t WHERE t.recipient=s.AT_address AND "
                        + "(t.block_height>s.cursor_height OR (t.block_height=s.cursor_height AND t.block_sequence>s.cursor_sequence))) "
                        + "WHERE s.AT_address=? AND s.schedule_class=1", address);
        } catch (SQLException e) { throw new DataException("Unable to update AT recipient index",e); }
    }

	public void checkConsistency() throws DataException {
		String sql = "SELECT COUNT(*) FROM ("
				+ "SELECT c.AT_address FROM ATCurrentState c LEFT OUTER JOIN ATBlockPayloads b ON b.block_height=c.latest_state_height "
				+ "LEFT OUTER JOIN ATRetainedStatePayloads r ON r.AT_address=c.AT_address AND r.state_height=c.latest_state_height "
				+ "AND r.payload_hash=c.payload_hash WHERE c.payload_hash IS NOT NULL AND b.block_height IS NULL AND r.AT_address IS NULL "
				+ "UNION ALL SELECT c.AT_address FROM ATCurrentState c LEFT OUTER JOIN ATBlockPayloads b ON b.block_height=c.previous_state_height "
				+ "LEFT OUTER JOIN ATRetainedStatePayloads r ON r.AT_address=c.AT_address AND r.state_height=c.previous_state_height "
				+ "AND r.payload_hash=c.previous_payload_hash WHERE c.previous_payload_hash IS NOT NULL AND b.block_height IS NULL AND r.AT_address IS NULL "
				+ "UNION ALL SELECT c.AT_address FROM ATCurrentState c LEFT OUTER JOIN ATBlockPayloads b ON b.block_height=c.rollback_state_height "
				+ "LEFT OUTER JOIN ATRetainedStatePayloads r ON r.AT_address=c.AT_address AND r.state_height=c.rollback_state_height "
				+ "AND r.payload_hash=c.rollback_payload_hash WHERE c.rollback_payload_hash IS NOT NULL "
				+ "AND b.block_height IS NULL AND r.AT_address IS NULL) missing";

		try (ResultSet resultSet = this.repository.checkedExecute(sql)) {
			if (resultSet == null)
				throw new DataException("Unable to check AT repository consistency");

			int atCount = resultSet.getInt(1);

			if (atCount > 0) {
				LOGGER.warn(() -> String.format("Missing %d latest AT state data row%s!", atCount, (atCount != 1 ? "s" : "")));
				LOGGER.warn("Export key data then resync using bootstrap as soon as possible");
			}
		} catch (SQLException e) {
			throw new DataException("Unable to check AT repository consistency", e);
		}

		try (ResultSet resultSet = this.repository.checkedExecute(
				"SELECT AT_address,latest_state_height,payload_hash,previous_state_height,previous_payload_hash,"
						+ "rollback_state_height,rollback_payload_hash "
						+ "FROM ATCurrentState WHERE payload_hash IS NOT NULL")) {
			if (resultSet != null) do {
				String address = resultSet.getString(1);
				int height = resultSet.getInt(2);
				byte[] hash = resultSet.getBytes(3);
				store.state(address, height, hash);
				Integer previousHeight = (Integer) resultSet.getObject(4);
				byte[] previousHash = resultSet.getBytes(5);
				if (previousHeight != null && previousHash != null)
					store.state(address, previousHeight, previousHash);
				Integer rollbackHeight = (Integer) resultSet.getObject(6);
				byte[] rollbackHash = resultSet.getBytes(7);
				if (rollbackHeight != null && rollbackHash != null)
					store.state(address, rollbackHeight, rollbackHash);
			} while (resultSet.next());
		} catch (SQLException e) {
			throw new DataException("AT repository contains a corrupt or mismatched current payload", e);
		}
	}

}
