package org.qortal.repository.hsqldb;

import org.ciyam.at.Timestamp;
import org.qortal.at.ATStateBundle;
import org.qortal.crypto.Crypto;
import org.qortal.data.at.ATData;
import org.qortal.data.at.ATStateData;
import org.qortal.repository.DataException;

import java.sql.*;
import java.util.*;

/** Physical AT writes. All operations belong to the caller's repository transaction. */
final class HSQLDBATStore {
    static final ATImmutableCache CODE_CACHE = new ATImmutableCache(8L * 1024 * 1024);
    static final ATImmutableCache STATE_CACHE = new ATImmutableCache(56L * 1024 * 1024);
    private final HSQLDBRepository repository;

    HSQLDBATStore(HSQLDBRepository repository) { this.repository = repository; }

    byte[] code(byte[] hash) throws SQLException {
        byte[] bytes = CODE_CACHE.get(hash);
        if (bytes != null) return bytes;
        try (ResultSet rs = repository.checkedExecute("SELECT code_bytes FROM ATCode WHERE code_hash = ?", hash)) {
            if (rs == null) throw new SQLException("Missing AT code");
            bytes = rs.getBytes(1); verify(hash,bytes); CODE_CACHE.put(hash, bytes); return bytes;
        }
    }

    byte[] state(byte[] hash) throws SQLException {
		byte[] bytes = stateIfPresent(hash);
		if (bytes == null) throw new SQLException("Missing retained AT state payload");
		return bytes;
	}

	byte[] stateIfPresent(byte[] hash) throws SQLException {
        if (hash == null) return null;
        byte[] bytes = STATE_CACHE.get(hash);
        if (bytes != null) return bytes;
		try (ResultSet rs = repository.checkedExecute(
				"SELECT AT_address,CASE WHEN payload_hash=? THEN latest_state_height "
						+ "WHEN previous_payload_hash=? THEN previous_state_height ELSE rollback_state_height END "
						+ "FROM ATCurrentState WHERE payload_hash=? OR previous_payload_hash=? OR rollback_payload_hash=? LIMIT 1",
				hash, hash, hash, hash, hash)) {
			if (rs == null) return null;
			return stateIfPresent(rs.getString(1), rs.getInt(2), hash);
		}
	}

	byte[] state(String address, int height, byte[] hash) throws SQLException {
		byte[] bytes = stateIfPresent(address, height, hash);
		if (bytes == null) throw new SQLException("Missing retained AT state payload");
		return bytes;
	}

	byte[] stateIfPresent(String address, int height, byte[] hash) throws SQLException {
		if (hash == null) return null;
		byte[] cached = STATE_CACHE.get(hash);
		if (cached != null) return cached;
		byte[] bundle = readPayloadBundle(height);
		if (bundle != null) {
			try {
				Map<String, ATStateBundle.Entry> entries = ATStateBundle.deserialize(bundle);
				for (ATStateBundle.Entry entry : entries.values()) STATE_CACHE.put(entry.hash, entry.data);
				ATStateBundle.Entry entry = entries.get(address);
				if (entry == null || !Arrays.equals(hash, entry.hash))
					throw new SQLException("AT state bundle does not match current head");
				return entry.data.clone();
			} catch (DataException e) {
				throw new SQLException("Invalid AT state bundle at height " + height, e);
			}
		}
		return readRetainedPayload(address, height, hash);
	}

    void preloadStates(List<ATStateData> metadata) throws SQLException {
        Map<org.qortal.utils.ByteArray, byte[]> missing = new LinkedHashMap<>();
        for (ATStateData state : metadata) {
            byte[] hash = state.getStateData();
            if (hash != null && STATE_CACHE.get(hash) == null) missing.put(org.qortal.utils.ByteArray.wrap(hash),hash);
        }
        if (missing.isEmpty()) return;
        Set<Integer> heights = metadata.stream().map(ATStateData::getHeight).collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
		for (int start = 0; start < heights.size(); start += 256) {
			List<Integer> chunk = new ArrayList<>(heights).subList(start, Math.min(start + 256, heights.size()));
			try (ResultSet rs = repository.checkedExecute("SELECT payload_bytes FROM ATBlockPayloads WHERE block_height IN ("
					+ String.join(",", Collections.nCopies(chunk.size(), "?")) + ") AND payload_bytes IS NOT NULL", chunk.toArray())) {
				if (rs != null) do {
					try {
						byte[] bundle = rs.getBytes(1);
						for (ATStateBundle.Entry entry : ATStateBundle.deserialize(bundle).values()) {
							verify(entry.hash, entry.data);
							STATE_CACHE.put(entry.hash, entry.data);
							missing.remove(org.qortal.utils.ByteArray.wrap(entry.hash));
						}
					} catch (DataException e) { throw new SQLException("Invalid AT state bundle", e); }
				} while (rs.next());
			}
		}
		if (!missing.isEmpty()) {
			List<String> addresses = metadata.stream().map(ATStateData::getATAddress).distinct()
					.collect(java.util.stream.Collectors.toList());
			for (int start = 0; start < addresses.size(); start += 256) {
				List<String> chunk = addresses.subList(start, Math.min(start + 256, addresses.size()));
				try (ResultSet rs = repository.checkedExecute(
						"SELECT AT_address,state_height,payload_hash,payload_bytes FROM ATRetainedStatePayloads "
								+ "WHERE AT_address IN (" + String.join(",", Collections.nCopies(chunk.size(), "?")) + ")",
						chunk.toArray())) {
					if (rs != null) do {
						byte[] hash = rs.getBytes(3), data = rs.getBytes(4);
						verify(hash, data);
						STATE_CACHE.put(hash, data);
						missing.remove(org.qortal.utils.ByteArray.wrap(hash));
					} while (rs.next());
				}
			}
		}
        if (!missing.isEmpty()) throw new SQLException("Missing retained AT state payloads");
    }

	private byte[] readRetainedPayload(String address, int height, byte[] hash) throws SQLException {
		try (ResultSet rs = repository.checkedExecute(
				"SELECT payload_hash,payload_bytes FROM ATRetainedStatePayloads WHERE AT_address=? AND state_height=?",
				address, height)) {
			if (rs == null) return null;
			byte[] storedHash = rs.getBytes(1), data = rs.getBytes(2);
			if (!Arrays.equals(hash, storedHash))
				throw new SQLException("Retained AT payload hash does not match current head");
			verify(storedHash, data);
			STATE_CACHE.put(storedHash, data);
			return data.clone();
		}
	}

	/**
	 * Copy only entries still named by current-state pointers out of an expiring block
	 * bundle. The caller deletes the bundle in the same repository transaction.
	 */
	Set<String> retainReferencedPayloads(Map<Integer, byte[]> bundles) throws SQLException, DataException {
		if (bundles.isEmpty()) return Collections.emptySet();
		Map<Integer, Map<String, ATStateBundle.Entry>> decoded = new HashMap<>();
		Map<String, Object[]> retained = new LinkedHashMap<>();
		Set<String> retainedAddresses = new HashSet<>();
		try (ResultSet rows = repository.checkedExecute(
				"SELECT AT_address,latest_state_height,payload_hash,previous_state_height,previous_payload_hash,"
						+ "rollback_state_height,rollback_payload_hash FROM ATCurrentState")) {
			if (rows != null) do {
				String address = rows.getString(1);
				for (int pair = 0; pair < 3; ++pair) {
					Integer pointerHeight = (Integer) rows.getObject(2 + pair * 2);
					byte[] pointerHash = rows.getBytes(3 + pair * 2);
					if (pointerHeight != null && pointerHash != null && bundles.containsKey(pointerHeight)) {
						Map<String, ATStateBundle.Entry> bundle = decoded.get(pointerHeight);
						if (bundle == null) {
							bundle = ATStateBundle.deserialize(bundles.get(pointerHeight));
							decoded.put(pointerHeight, bundle);
						}
						ATStateBundle.Entry entry = bundle.get(address);
						if (entry == null || !Arrays.equals(pointerHash, entry.hash))
							throw new SQLException("Referenced AT payload is missing from block bundle at height " + pointerHeight);
						verify(entry.hash, entry.data);
						String key = address + '\0' + pointerHeight;
						Object[] prior = retained.putIfAbsent(key,
								new Object[]{address, pointerHeight, entry.hash, entry.data});
						if (prior != null && (!Arrays.equals((byte[]) prior[2], entry.hash)
								|| !Arrays.equals((byte[]) prior[3], entry.data)))
							throw new SQLException("Conflicting AT pointers at retained height " + pointerHeight);
						retainedAddresses.add(address);
					}
				}
			} while (rows.next());
		}
		if (!retained.isEmpty())
			repository.executeCheckedBatchUpdate(
					"INSERT INTO ATRetainedStatePayloads (AT_address,state_height,payload_hash,payload_bytes) VALUES(?,?,?,?)",
					new ArrayList<>(retained.values()));
		return retainedAddresses;
	}

	void deleteUnreferencedRetainedPayloads(Collection<String> addresses) throws SQLException {
		if (addresses.isEmpty()) return;
		List<String> unique = new ArrayList<>(new LinkedHashSet<>(addresses));
		for (int start = 0; start < unique.size(); start += 256) {
			List<String> chunk = unique.subList(start, Math.min(start + 256, unique.size()));
			repository.executeCheckedUpdate(
					"DELETE FROM ATRetainedStatePayloads d WHERE d.AT_address IN ("
							+ String.join(",", Collections.nCopies(chunk.size(), "?")) + ") AND NOT EXISTS ("
							+ "SELECT 1 FROM ATCurrentState c WHERE c.AT_address=d.AT_address AND ("
							+ "(c.latest_state_height=d.state_height AND c.payload_hash=d.payload_hash) OR "
							+ "(c.previous_state_height=d.state_height AND c.previous_payload_hash=d.payload_hash) OR "
							+ "(c.rollback_state_height=d.state_height AND c.rollback_payload_hash=d.payload_hash)))",
					chunk.toArray());
		}
	}

    void saveDefinition(ATData data) throws SQLException, DataException {
        if (!repository.exists("ATDefinitions", "AT_address = ?", data.getATAddress())) {
            byte[] bytes = data.getCodeBytes(), hash = data.getCodeHash();
            verify(hash, bytes);
            try (ResultSet existing = repository.checkedExecute("SELECT code_bytes FROM ATCode WHERE code_hash = ?", hash)) {
                if (existing == null)
                    repository.executeCheckedUpdate("INSERT INTO ATCode VALUES(?,?)", hash, bytes);
                else if (!Arrays.equals(bytes, existing.getBytes(1))) throw new SQLException("AT code collision/corruption");
            }
            CODE_CACHE.put(hash, bytes);
            new HSQLDBSaver("ATDefinitions").bind("AT_address", data.getATAddress()).bind("creator", data.getCreatorPublicKey())
                    .bind("created_when", data.getCreation()).bind("version", data.getVersion()).bind("asset_id", data.getAssetId())
                    .bind("code_bytes", bytes).bind("code_hash", hash)
                    .bind("is_sleeping", data.getIsSleeping()).bind("sleep_until_height", data.getSleepUntilHeight())
                    .bind("is_finished", data.getIsFinished()).bind("had_fatal_error", data.getHadFatalError())
                    .bind("is_frozen", data.getIsFrozen()).bind("frozen_balance", data.getFrozenBalance())
                    .bind("sleep_until_message_timestamp", data.getSleepUntilMessageTimestamp()).execute(repository);
        }
        saveHeads(Collections.singletonList(data));
    }

    void saveHeads(List<ATData> data) throws SQLException, DataException {
        if (data.isEmpty()) return;
        String sql = "INSERT INTO ATCurrentState (AT_address,is_sleeping,sleep_until_height,is_finished,had_fatal_error,is_frozen,"
                + "frozen_balance,sleep_until_message_timestamp,cursor_height,cursor_sequence,schedule_class,message_pending) "
                + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE is_sleeping=?,sleep_until_height=?,is_finished=?,"
                + "had_fatal_error=?,is_frozen=?,frozen_balance=?,sleep_until_message_timestamp=?,cursor_height=?,cursor_sequence=?,"
                + "schedule_class=?,message_pending=?";
        HSQLDBRepository.CHECKPOINT_GATE.readLock().lock();
        try {
            repository.markTransactionStarted();
            PreparedStatement ps = repository.prepareStatement(sql);
            for (ATData at : data) {
                Timestamp cursor = at.getSleepUntilMessageTimestamp() == null ? null : new Timestamp(at.getSleepUntilMessageTimestamp());
                boolean pending = cursor != null && repository.getATRepository().findNextTransaction(at.getATAddress(), cursor.blockHeight, cursor.transactionSequence) != null;
                Object[] flags = { at.getIsSleeping(), at.getSleepUntilHeight(), at.getIsFinished(), at.getHadFatalError(), at.getIsFrozen(),
                        at.getFrozenBalance(), at.getSleepUntilMessageTimestamp(), cursor == null ? null : cursor.blockHeight,
                        cursor == null ? null : cursor.transactionSequence, at.getIsFinished() ? 2 : cursor == null ? 0 : 1, pending };
                ps.setString(1, at.getATAddress());
                for (int i = 0; i < flags.length; ++i) { ps.setObject(i + 2, flags[i]); ps.setObject(i + 13, flags[i]); }
                ps.addBatch();
            }
            ps.executeBatch();
        } finally { HSQLDBRepository.CHECKPOINT_GATE.readLock().unlock(); }
    }

    /** Persist a block result blob, store new payloads, and update ATCurrentState. */
    void saveBlockResult(int height, byte[] blob, List<ATData> heads, List<ATStateData> states) throws SQLException, DataException {
        HSQLDBRepository.CHECKPOINT_GATE.readLock().lock();
		try {
            repository.markTransactionStarted();
			Map<String, ATStateBundle.Entry> bundleEntries = new LinkedHashMap<>();
			byte[] existingResult = readBlockResult(height);
			byte[] existingPayload = readPayloadBundle(height);
			if (existingPayload != null)
				bundleEntries.putAll(ATStateBundle.deserialize(existingPayload));
			for (ATStateData state : states) {
				byte[] bytes = state.getStateData();
				if (bytes == null) {
					try (ResultSet rs = repository.checkedExecute(
							"SELECT latest_state_height,payload_hash FROM ATCurrentState WHERE AT_address=?", state.getATAddress())) {
						if (rs == null || !Arrays.equals(state.getStateHash(), rs.getBytes(2)))
							throw new SQLException("Referenced AT state does not match current head");
						bytes = state(state.getATAddress(), rs.getInt(1), state.getStateHash());
					}
				}
				verify(state.getStateHash(), bytes);
				ATStateBundle.Entry addition = new ATStateBundle.Entry(state.getStateHash(), bytes);
				ATStateBundle.Entry existing = bundleEntries.putIfAbsent(state.getATAddress(), addition);
				if (existing != null && (!Arrays.equals(existing.hash, addition.hash) || !Arrays.equals(existing.data, addition.data)))
					throw new SQLException("Conflicting AT state in block result");
			}
			blob = mergeBlockResults(existingResult, blob, height);
			byte[] payloadBundle;
			try { payloadBundle = ATStateBundle.serialize(bundleEntries); }
			catch (DataException e) { throw new SQLException("Unable to build AT state bundle", e); }
			if (repository.executeCheckedUpdate("UPDATE ATBlockResults SET result_bytes=? WHERE block_height=?", blob, height) == 0)
				repository.executeCheckedUpdate("INSERT INTO ATBlockResults(block_height,result_bytes) VALUES(?,?)", height, blob);
			if (repository.executeCheckedUpdate("UPDATE ATBlockPayloads SET payload_bytes=? WHERE block_height=?", payloadBundle, height) == 0)
				repository.executeCheckedUpdate("INSERT INTO ATBlockPayloads(block_height,payload_bytes) VALUES(?,?)", height, payloadBundle);

            Map<String, Integer> currentHeights = new HashMap<>();
            List<String> referenceAddresses = states.stream()
                    .filter(state -> state instanceof org.qortal.data.at.ATStateReference)
                    .map(ATStateData::getATAddress).collect(java.util.stream.Collectors.toList());
            for (int start = 0; start < referenceAddresses.size(); start += 256) {
                List<String> chunk = referenceAddresses.subList(start, Math.min(start + 256, referenceAddresses.size()));
                try (ResultSet rs = repository.checkedExecute(
                        "SELECT AT_address,latest_state_height FROM ATCurrentState WHERE AT_address IN ("
                                + String.join(",", Collections.nCopies(chunk.size(), "?")) + ")", chunk.toArray())) {
                    if (rs != null) do { currentHeights.put(rs.getString(1), (Integer) rs.getObject(2)); } while (rs.next());
                }
            }

            Map<String, ATStateData> statesByAddress = new HashMap<>();
            for (ATStateData state : states) {
                // ATStateReference requires the parent state height to match before reusing payload
                if (state instanceof org.qortal.data.at.ATStateReference) {
                    int parentHeight = ((org.qortal.data.at.ATStateReference) state).getParentStateHeight();
                    if (!Objects.equals(currentHeights.get(state.getATAddress()), parentHeight))
                        throw new SQLException("AT state reference parent height mismatch");
                }
                if (statesByAddress.put(state.getATAddress(), state) != null)
                    throw new SQLException("Duplicate AT state in block result");
            }

            String updateSql = "UPDATE ATCurrentState SET previous_state_height=latest_state_height,previous_payload_hash=payload_hash,"
					+ "latest_state_height=?,payload_hash=?,is_sleeping=?,sleep_until_height=?,"
                    + "is_finished=?,had_fatal_error=?,is_frozen=?,frozen_balance=?,sleep_until_message_timestamp=?,"
                    + "cursor_height=?,cursor_sequence=?,schedule_class=?,message_pending=? WHERE AT_address=?";
            List<Object[]> combinedHeads = new ArrayList<>(heads.size());
            for (ATData head : heads) {
                ATStateData state = statesByAddress.remove(head.getATAddress());
                if (state == null) throw new SQLException("Missing AT state for updated head " + head.getATAddress());
                Timestamp cursor = head.getSleepUntilMessageTimestamp() == null ? null : new Timestamp(head.getSleepUntilMessageTimestamp());
                boolean pending = cursor != null && repository.getATRepository()
                        .findNextTransaction(head.getATAddress(), cursor.blockHeight, cursor.transactionSequence) != null;
                combinedHeads.add(new Object[]{height, state.getStateHash(), head.getIsSleeping(), head.getSleepUntilHeight(),
                        head.getIsFinished(), head.getHadFatalError(), head.getIsFrozen(), head.getFrozenBalance(),
                        head.getSleepUntilMessageTimestamp(), cursor == null ? null : cursor.blockHeight,
                        cursor == null ? null : cursor.transactionSequence, head.getIsFinished() ? 2 : cursor == null ? 0 : 1,
                        pending, head.getATAddress()});
            }
            repository.executeCheckedBatchUpdate(updateSql, combinedHeads);
            if (!statesByAddress.isEmpty()) {
                List<Object[]> headOnly = statesByAddress.values().stream()
                        .map(state -> new Object[]{height, state.getStateHash(), state.getATAddress()})
                        .collect(java.util.stream.Collectors.toList());
                repository.executeCheckedBatchUpdate(
                        "UPDATE ATCurrentState SET previous_state_height=latest_state_height,previous_payload_hash=payload_hash,"
                                + "latest_state_height=?,payload_hash=? WHERE AT_address=?", headOnly);
            }
			for (ATStateBundle.Entry entry : bundleEntries.values()) STATE_CACHE.put(entry.hash, entry.data);
		} finally { HSQLDBRepository.CHECKPOINT_GATE.readLock().unlock(); }
    }

	/** Persist a deployment state directly into its block bundle. */
	void saveInitialState(ATStateData state) throws SQLException, DataException {
		byte[] data = state.getStateData();
		verify(state.getStateHash(), data);
		int height = state.getHeight();
		Map<String, ATStateBundle.Entry> payloads = new LinkedHashMap<>();
		List<org.qortal.at.ATBlockResult.Entry> results = new ArrayList<>();
		byte[] result = readBlockResult(height);
		byte[] payloadBundle = readPayloadBundle(height);
		if (result != null) results.addAll(org.qortal.at.ATBlockResult.deserialize(result));
		if (payloadBundle != null) payloads.putAll(ATStateBundle.deserialize(payloadBundle));
		for (org.qortal.at.ATBlockResult.Entry entry : results)
			if (entry.address.equals(state.getATAddress())) {
				if (!Arrays.equals(entry.stateHash, state.getStateHash()))
					throw new SQLException("Conflicting deployment AT result at height " + height);
				ATStateBundle.Entry payload = payloads.get(state.getATAddress());
				if (payload == null || !Arrays.equals(payload.hash, state.getStateHash()) || !Arrays.equals(payload.data, data))
					throw new SQLException("Missing deployment AT payload at height " + height);
				STATE_CACHE.put(state.getStateHash(), data);
				return;
			}
		results.add(new org.qortal.at.ATBlockResult.Entry(state.getATAddress(), state.getStateHash(), state.getFees(),
				true, state.getSleepUntilMessageTimestamp(), -1, null,
				false, null, false, false, false, null, null));
		ATStateBundle.Entry old = payloads.putIfAbsent(state.getATAddress(),
				new ATStateBundle.Entry(state.getStateHash(), data));
		if (old != null && (!Arrays.equals(old.hash, state.getStateHash()) || !Arrays.equals(old.data, data)))
			throw new SQLException("Conflicting deployment AT payload at height " + height);
		byte[] resultBytes = org.qortal.at.ATBlockResult.serialize(results);
		byte[] payloadBytes = ATStateBundle.serialize(payloads);
		if (repository.executeCheckedUpdate("UPDATE ATBlockResults SET result_bytes=? WHERE block_height=?", resultBytes, height) == 0)
			repository.executeCheckedUpdate("INSERT INTO ATBlockResults(block_height,result_bytes) VALUES(?,?)", height, resultBytes);
		if (repository.executeCheckedUpdate("UPDATE ATBlockPayloads SET payload_bytes=? WHERE block_height=?", payloadBytes, height) == 0)
			repository.executeCheckedUpdate("INSERT INTO ATBlockPayloads(block_height,payload_bytes) VALUES(?,?)", height, payloadBytes);
		STATE_CACHE.put(state.getStateHash(), data);
	}

	private byte[] mergeBlockResults(byte[] existing, byte[] incoming, int height) throws SQLException, DataException {
		if (existing == null) return incoming;
		Map<String, org.qortal.at.ATBlockResult.Entry> merged = new LinkedHashMap<>();
		for (org.qortal.at.ATBlockResult.Entry entry : org.qortal.at.ATBlockResult.deserialize(existing))
			merged.put(entry.address, entry);
		for (org.qortal.at.ATBlockResult.Entry entry : org.qortal.at.ATBlockResult.deserialize(incoming)) {
			org.qortal.at.ATBlockResult.Entry old = merged.putIfAbsent(entry.address, entry);
			if (old != null && !Arrays.equals(old.stateHash, entry.stateHash))
				throw new SQLException("Conflicting AT block result at height " + height + " for " + entry.address);
		}
		return org.qortal.at.ATBlockResult.serialize(new ArrayList<>(merged.values()));
	}

    byte[] readBlockResult(int height) throws SQLException {
		try (ResultSet rs = repository.checkedExecute("SELECT result_bytes FROM ATBlockResults WHERE block_height=?", height)) {
			return rs == null ? null : rs.getBytes(1);
        }
    }

	private byte[] readPayloadBundle(int height) throws SQLException {
		try (ResultSet rs = repository.checkedExecute("SELECT payload_bytes FROM ATBlockPayloads WHERE block_height=?", height)) {
			return rs == null ? null : rs.getBytes(1);
		}
	}

	private boolean hasPersistedState(String address, int height, byte[] hash) throws SQLException {
		byte[] bundle = readPayloadBundle(height);
		if (bundle != null) {
			try {
				ATStateBundle.Entry entry = ATStateBundle.deserialize(bundle).get(address);
				return entry != null && Arrays.equals(hash, entry.hash);
			} catch (DataException e) {
				throw new SQLException("Invalid AT state bundle at height " + height, e);
			}
		}
		return readRetainedPayload(address, height, hash) != null;
	}

    /**
     * Delete a block result and restore ATCurrentState from the stored previous state info.
     *
     * @return true when the versioned result restored all mutable metadata, or false when
     *         the legacy path still requires metadata restoration from parent payloads
     */
    boolean deleteBlockResult(int height) throws SQLException, DataException {
        byte[] blob = readBlockResult(height);
        if (blob == null) {
            restoreLegacyBlockResult(height);
            return false;
        }
        List<org.qortal.at.ATBlockResult.Entry> entries = org.qortal.at.ATBlockResult.deserialize(blob);

		// A block normally contains hundreds of AT entries that all reference the same parent
		// block. Load and verify each parent bundle/result once, then use address-indexed maps.
		// This keeps rollback work linear in the number of entries instead of repeatedly
		// deserializing and hashing the same block-level blobs for every AT.
		Set<Integer> parentHeights = new LinkedHashSet<>();
		for (org.qortal.at.ATBlockResult.Entry entry : entries)
			if (!entry.isInitial && entry.prevStateHash != null)
				parentHeights.add(entry.prevHeight);

		Map<Integer, Map<String, ATStateBundle.Entry>> payloadsByHeight = new HashMap<>();
		Map<Integer, Map<String, org.qortal.at.ATBlockResult.Entry>> resultsByHeight = new HashMap<>();
		for (Integer parentHeight : parentHeights) {
			byte[] payloadBlob = readPayloadBundle(parentHeight);
			payloadsByHeight.put(parentHeight, payloadBlob == null
					? Collections.emptyMap() : ATStateBundle.deserialize(payloadBlob));

			Map<String, org.qortal.at.ATBlockResult.Entry> parentResults = new HashMap<>();
			byte[] parentResultBlob = readBlockResult(parentHeight);
			if (parentResultBlob != null)
				for (org.qortal.at.ATBlockResult.Entry parent : org.qortal.at.ATBlockResult.deserialize(parentResultBlob))
					parentResults.put(parent.address, parent);
			resultsByHeight.put(parentHeight, parentResults);
		}
		Set<Integer> grandparentHeights = new LinkedHashSet<>();
		for (Map<String, org.qortal.at.ATBlockResult.Entry> parentResults : resultsByHeight.values())
			for (org.qortal.at.ATBlockResult.Entry parent : parentResults.values())
				if (!parent.isInitial && parent.prevStateHash != null && !payloadsByHeight.containsKey(parent.prevHeight))
					grandparentHeights.add(parent.prevHeight);
		for (Integer grandparentHeight : grandparentHeights) {
			byte[] payloadBlob = readPayloadBundle(grandparentHeight);
			if (payloadBlob != null) payloadsByHeight.put(grandparentHeight, ATStateBundle.deserialize(payloadBlob));
		}

		Map<String, StatePointer> parentPointers = new HashMap<>();
		for (org.qortal.at.ATBlockResult.Entry entry : entries)
			if (!entry.isInitial && entry.prevStateHash != null) {
				ATStateBundle.Entry payload = payloadsByHeight.getOrDefault(entry.prevHeight, Collections.emptyMap())
						.get(entry.address);
				byte[] parentPayload = payload == null
						? readRetainedPayload(entry.address, entry.prevHeight, entry.prevStateHash) : payload.data;
				if (parentPayload == null || (payload != null && !Arrays.equals(entry.prevStateHash, payload.hash)))
					throw new SQLException("Cannot orphan AT block: retained parent payload is missing for " + entry.address);

				org.qortal.at.ATBlockResult.Entry parent = resultsByHeight.get(entry.prevHeight).get(entry.address);
				StatePointer pointer;
				if (parent == null) {
					pointer = previousPointerLegacy(entry.address, entry.prevHeight);
				} else if (parent.isInitial || parent.prevStateHash == null) {
					pointer = null;
				} else {
					ATStateBundle.Entry grandparent = payloadsByHeight
							.getOrDefault(parent.prevHeight, Collections.emptyMap()).get(entry.address);
					byte[] grandparentPayload = grandparent == null
							? readRetainedPayload(entry.address, parent.prevHeight, parent.prevStateHash) : grandparent.data;
					pointer = grandparentPayload != null && (grandparent == null
							|| Arrays.equals(parent.prevStateHash, grandparent.hash))
							? new StatePointer(parent.prevHeight, parent.prevStateHash) : null;
				}
				parentPointers.put(entry.address, pointer);
			}

		List<Object[]> deletions = new ArrayList<>();
		List<Object[]> updates = new ArrayList<>();
        for (org.qortal.at.ATBlockResult.Entry entry : entries) {
            if (entry.isInitial) {
				deletions.add(new Object[]{entry.address});
            } else {
                byte[] prevStateHash = entry.prevStateHash;
                Timestamp cursor = entry.prevSleepUntilMessageTimestamp == null ? null
                    : new Timestamp(entry.prevSleepUntilMessageTimestamp);
                int scheduleClass = entry.prevIsFinished ? 2 : cursor == null ? 0 : 1;
                boolean messagePending = cursor != null && repository.getATRepository()
                    .findNextTransaction(entry.address, cursor.blockHeight, cursor.transactionSequence) != null;
				StatePointer previous = parentPointers.get(entry.address);
				updates.add(new Object[]{
                    entry.prevHeight >= 0 ? entry.prevHeight : null, prevStateHash,
                    entry.prevIsSleeping, entry.prevSleepUntilHeight,
                    entry.prevIsFinished, entry.prevHadFatalError, entry.prevIsFrozen, entry.prevFrozenBalance,
                    entry.prevSleepUntilMessageTimestamp,
                    cursor == null ? null : cursor.blockHeight, cursor == null ? null : cursor.transactionSequence,
                    scheduleClass, messagePending, previous == null ? null : previous.height,
					previous == null ? null : previous.hash, entry.address});
            }
        }
		repository.executeCheckedBatchUpdate("DELETE FROM ATCurrentState WHERE AT_address=?", deletions);
		repository.executeCheckedBatchUpdate(
				"UPDATE ATCurrentState SET latest_state_height=?,payload_hash=?,is_sleeping=?,sleep_until_height=?,"
				+ "is_finished=?,had_fatal_error=?,is_frozen=?,frozen_balance=?,sleep_until_message_timestamp=?,"
				+ "cursor_height=?,cursor_sequence=?,schedule_class=?,message_pending=?,previous_state_height=?,previous_payload_hash=? "
				+ "WHERE AT_address=?", updates);
		repository.delete("ATBlockResults", "block_height=?", height);
		repository.delete("ATBlockPayloads", "block_height=?", height);
		repository.delete("ATStates", "height=?", height);
		return true;
    }

	private StatePointer previousPointerLegacy(String address, int height) throws SQLException {
		try (ResultSet rs = repository.checkedExecute("SELECT height,payload_hash FROM ATStates WHERE AT_address=? AND height<? "
				+ "ORDER BY AT_address DESC,height DESC LIMIT 1", address, height)) {
			if (rs == null) return null;
			int previousHeight = rs.getInt(1);
			byte[] previousHash = rs.getBytes(2);
			return previousHash != null && hasPersistedState(address, previousHeight, previousHash)
					? new StatePointer(previousHeight, previousHash) : null;
		}
	}

	private static final class StatePointer {
		private final int height;
		private final byte[] hash;
		private StatePointer(int height, byte[] hash) { this.height = height; this.hash = hash; }
	}

    /**
     * Blocks that predate the store migration have no ATBlockResults row. Their compact
     * ATStates metadata is deliberately retained so an orphan crossing the migration
     * boundary can still move every current head to its exact parent payload.
     */
    private void restoreLegacyBlockResult(int height) throws SQLException {
        List<Object[]> parents = new ArrayList<>();
		try (ResultSet rs = repository.checkedExecute(
				"SELECT h.AT_address,h.is_initial,p.height,p.payload_hash,p.sleep_until_message_timestamp,"
						+ "pp.height,pp.payload_hash "
                        + "FROM ATStates h LEFT JOIN LATERAL (SELECT s.height,s.payload_hash,s.sleep_until_message_timestamp FROM ATStates s "
                        + "WHERE s.AT_address=h.AT_address AND s.height<h.height "
                        + "ORDER BY s.AT_address DESC,s.height DESC LIMIT 1) p ON TRUE "
						+ "LEFT JOIN LATERAL (SELECT s.height,s.payload_hash FROM ATStates s "
						+ "WHERE s.AT_address=h.AT_address AND s.height<p.height "
						+ "ORDER BY s.AT_address DESC,s.height DESC LIMIT 1) pp ON TRUE "
                        + "WHERE h.height=? ORDER BY h.AT_address", height)) {
            if (rs == null)
                return;
            do {
				parents.add(new Object[]{rs.getString(1), rs.getBoolean(2), rs.getObject(3), rs.getBytes(4),
						rs.getObject(5), rs.getObject(6), rs.getBytes(7)});
            } while (rs.next());
        }

		try (PreparedStatement delete = repository.prepareStatement("DELETE FROM ATCurrentState WHERE AT_address=?");
             PreparedStatement update = repository.prepareStatement(
                     "UPDATE ATCurrentState SET latest_state_height=?,payload_hash=?,sleep_until_message_timestamp=?,"
							 + "previous_state_height=?,previous_payload_hash=? WHERE AT_address=?")) {
            int deleteCount = 0, updateCount = 0;
            for (Object[] parent : parents) {
                String address = (String) parent[0];
                if ((Boolean) parent[1]) {
                    delete.setString(1, address);
                    delete.addBatch();
                    ++deleteCount;
                    continue;
                }
                if (parent[2] == null || parent[3] == null)
                    throw new SQLException("Cannot orphan legacy AT block: parent state is missing for " + address);
				if (!hasPersistedState(address, (Integer) parent[2], (byte[]) parent[3]))
                    throw new SQLException("Cannot orphan legacy AT block: parent payload is missing for " + address);
				Integer previousHeight = (Integer) parent[5];
				byte[] previousHash = (byte[]) parent[6];
				if (previousHeight != null && previousHash != null
						&& !hasPersistedState(address, previousHeight, previousHash)) {
					previousHeight = null;
					previousHash = null;
				}
                update.setInt(1, (Integer) parent[2]);
                update.setBytes(2, (byte[]) parent[3]);
                update.setObject(3, parent[4]);
				update.setObject(4, previousHeight);
				update.setBytes(5, previousHash);
				update.setString(6, address);
                update.addBatch();
                ++updateCount;
            }
            if (deleteCount != 0) delete.executeBatch();
            if (updateCount != 0) update.executeBatch();
        }
		repository.delete("ATBlockResults", "block_height=?", height);
		repository.delete("ATBlockPayloads", "block_height=?", height);
        repository.delete("ATStates", "height=?", height);
    }

    static void verify(byte[] hash, byte[] bytes) throws SQLException {
        if (bytes == null || !Arrays.equals(hash, Crypto.digest(bytes))) throw new SQLException("Invalid AT payload hash");
    }
}
