package org.qortal.repository;

import org.qortal.data.at.ATData;
import org.qortal.data.at.ATStateData;
import org.qortal.utils.ByteArray;

import java.util.List;
import java.util.Set;

public interface ATRepository {

    List<org.qortal.data.at.ATExecutionState> getExecutionStates(List<String> addresses) throws DataException;
    byte[] getStatePayload(byte[] hash) throws DataException;
    byte[] getStatePayload(String address, int height, byte[] hash) throws DataException;
    byte[] getCodePayload(byte[] hash) throws DataException;

	// CIYAM AutomatedTransactions

	/** Returns ATData using AT's address or null if none found */
	public ATData fromATAddress(String atAddress) throws DataException;

	public List<ATData> fromATAddresses(List<String> atAddresses) throws DataException;

	/** Returns where AT with passed address exists in repository */
	public boolean exists(String atAddress) throws DataException;

	/** Returns AT creator's public key, or null if not found */
	public byte[] getCreatorPublicKey(String atAddress) throws DataException;

	/** Returns list of executable ATs, empty if none found */
	public List<ATData> getAllExecutableATs() throws DataException;

	/** Candidates selected against the current repository view, in canonical order; code bytes are loaded lazily. */
	public List<ATData> getATsForExecution(int candidateHeight) throws DataException;

    /** Persist a pre-serialized block result blob, store new payloads, and update ATCurrentState. */
    public void saveBlockResult(int height, byte[] blob, List<ATData> updatedHeads, List<ATStateData> states) throws DataException;

    /** Read a block result blob by height, or null if none. */
    public byte[] getBlockResult(int height) throws DataException;

    /** Delete a block result by height and restore affected ATCurrentState entries. */
    public void deleteBlockResult(int height) throws DataException;

    /** Update mutable AT flags after restoring a block's parent receipts. */
    public void saveATMetadata(List<ATData> ats) throws DataException;

	/** Returns list of ATs with matching code hash, optionally executable only. */
	public List<ATData> getATsByFunctionality(byte[] codeHash, Boolean isExecutable, Integer limit, Integer offset, Boolean reverse) throws DataException;

	/** Returns list of all ATs matching one of passed code hashes, optionally executable only. */
	public List<ATData> getAllATsByFunctionality(Set<ByteArray> codeHashes, Boolean isExecutable) throws DataException;

	/**
	 * Returns current AT definitions matching one of the supplied code hashes whose current-state
	 * head is at or above {@code minimumLatestStateHeight}. A null height returns all matches.
	 */
	public List<ATData> getAllATsByFunctionalitySince(Set<ByteArray> codeHashes, Boolean isExecutable,
			Integer minimumLatestStateHeight) throws DataException;

	/** Returns creation block height given AT's address or null if not found */
	public Integer getATCreationBlockHeight(String atAddress) throws DataException;

	/** Saves ATData into repository */
	public void save(ATData atData) throws DataException;

	/** Removes an AT from repository, including associated ATStateData */
	public void delete(String atAddress) throws DataException;

	/** Set the initial or latest state head in ATCurrentState for a newly deployed AT. */
	public void setCurrentStateHead(String atAddress, int height, byte[] payloadHash) throws DataException;

	// AT States

	/**
	 * Returns ATStateData for an AT at given height.
	 * 
	 * @param atAddress
	 *            - AT's address
	 * @param height
	 *            - block height
	 * @return ATStateData for AT at given height or null if none found
	 */
	public ATStateData getATStateAtHeight(String atAddress, int height) throws DataException;

	/**
	 * Returns latest ATStateData for an AT.
	 * <p>
	 * As ATs don't necessarily run every block, this will return the <tt>ATStateData</tt> with the greatest height.
	 * 
	 * @param atAddress
	 *            - AT's address
	 * @return ATStateData for AT with greatest height or null if none found
	 */
	public ATStateData getLatestATState(String atAddress) throws DataException;

	public List<ATStateData> getLatestATStates(List<String> collect) throws DataException;

	/**
	 * Returns final ATStateData for ATs matching codeHash (required)
	 * and specific data segment value (optional).
	 * <p>
	 * If searching for specific data segment value, both <tt>dataByteOffset</tt>
	 * and <tt>expectedValue</tt> need to be non-null.
	 * <p>
	 * Note that <tt>dataByteOffset</tt> starts from 0 and will typically be
	 * a multiple of <tt>MachineState.VALUE_SIZE</tt>, which is usually 8:
	 * width of a long.
	 * <p>
	 * Although <tt>expectedValue</tt>, if provided, is natively an unsigned long,
	 * the data segment comparison is done via unsigned hex string.
	 */
	public List<ATStateData> getMatchingFinalATStates(byte[] codeHash, byte[] buyerPublicKey, byte[] sellerPublicKey, Boolean isFinished,
													  Integer dataByteOffset, Long expectedValue, Integer minimumFinalHeight,
													  Integer limit, Integer offset, Boolean reverse) throws DataException;

	/**
	 * Returns final ATStateData for ATs matching codeHash (required)
	 * and specific data segment value (optional), returning at least
	 * <tt>minimumCount</tt> entries over a span of at least
	 * <tt>minimumPeriod</tt> ms, given enough entries in repository.
	 * <p>
	 * If searching for specific data segment value, both <tt>dataByteOffset</tt>
	 * and <tt>expectedValue</tt> need to be non-null.
	 * <p>
	 * Note that <tt>dataByteOffset</tt> starts from 0 and will typically be
	 * a multiple of <tt>MachineState.VALUE_SIZE</tt>, which is usually 8:
	 * width of a long.
	 * <p>
	 * Although <tt>expectedValue</tt>, if provided, is natively an unsigned long,
	 * the data segment comparison is done via unsigned hex string.
	 */
	public List<ATStateData> getMatchingFinalATStatesQuorum(byte[] codeHash, Boolean isFinished,
			Integer dataByteOffset, Long expectedValue,
			int minimumCount, int maximumCount, long minimumPeriod) throws DataException;

	/**
	 * Returns all ATStateData for a given block height.
	 * <p>
	 * Unlike <tt>getATState</tt>, only returns <i>partial</i> ATStateData saved at the given height.
	 *
	 * @param height
	 *            - block height
	 * @return list of ATStateData for given height, empty list if none found
	 * @throws DataException
	 */
	public List<ATStateData> getBlockATStatesAtHeight(int height) throws DataException;


	/** Returns height of first trimmable AT state. */
	public int getAtTrimHeight() throws DataException;

	/** Sets new base height for AT state trimming.
	 * <p>
	 * NOTE: performs implicit <tt>repository.saveChanges()</tt>.
	 */
	public void setAtTrimHeight(int trimHeight) throws DataException;

	/** Trims full AT state data between passed heights. Returns number of trimmed rows. */
	public int trimAtStates(int minHeight, int maxHeight, int limit) throws DataException;

	/** Result from a bounded AT payload trim, including whether the requested range is complete. */
	public static final class ATTrimResult {
		private final int trimmed;
		private final boolean rangeComplete;

		public ATTrimResult(int trimmed, boolean rangeComplete) {
			this.trimmed = trimmed;
			this.rangeComplete = rangeComplete;
		}

		public int getTrimmed() {
			return this.trimmed;
		}

		public boolean isRangeComplete() {
			return this.rangeComplete;
		}
	}

	/**
	 * Trims a bounded range and reports when every releasable payload in that range
	 * was examined. Legacy cleanup performed by the same call does not hold back
	 * this completion signal.
	 */
	public ATTrimResult trimAtStatesDetailed(int minHeight, int maxHeight, int limit) throws DataException;


	/** Returns height of first prunable AT state. */
	public int getAtPruneHeight() throws DataException;

	/** Sets new base height for AT state pruning.
	 * <p>
	 * NOTE: performs implicit <tt>repository.saveChanges()</tt>.
	 */
	public void setAtPruneHeight(int pruneHeight) throws DataException;

	/** Prunes full AT state data between passed heights. Returns number of pruned rows. */
	public int pruneAtStates(int minHeight, int maxHeight) throws DataException;


	/** Persist the initial state produced by an AT deployment. */
	public void saveInitialState(ATStateData atStateData) throws DataException;

	/** Store an archive V1 AT receipt summary. Archive records never contain VM payload bytes. */
	public void saveArchivedStateSummary(ATStateData atStateData) throws DataException;

	/** Delete state data for all ATs at this height */
	/**
	 * Delete a block's AT states and restore their current-state heads.
	 *
	 * @return true when the block result restored all mutable AT metadata, or false when
	 *         the legacy state path still requires metadata restoration from parent payloads
	 */
	public boolean deleteATStates(int height) throws DataException;

	// Finding transactions for ATs to process

	static class NextTransactionInfo {
		public final int height;
		public final int sequence;
		public final byte[] signature;

		public NextTransactionInfo(int height, int sequence, byte[] signature) {
			this.height = height;
			this.sequence = sequence;
			this.signature = signature;
		}
	}

	/**
	 * Find next transaction for AT to process.
	 * <p>
	 * @param recipient AT address
	 * @param height starting height
	 * @param sequence starting sequence
	 * @return next transaction info, or null if none found
	 */
	public NextTransactionInfo findNextTransaction(String recipient, int height, int sequence) throws DataException;

	// Other

	public void checkConsistency() throws DataException;

}
