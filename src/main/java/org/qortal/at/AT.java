package org.qortal.at;

import org.ciyam.at.MachineState;
import org.ciyam.at.Timestamp;
import org.qortal.crypto.Crypto;
import org.qortal.data.at.ATData;
import org.qortal.data.at.ATStateData;
import org.qortal.data.transaction.DeployAtTransactionData;
import org.qortal.repository.ATRepository;
import org.qortal.repository.DataException;
import org.qortal.repository.Repository;
import org.qortal.transaction.AtTransaction;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class AT {

	// Properties
	private Repository repository;
	private ATData atData;
	private ATStateData atStateData;
	private MachineState resultingState;
	private boolean unchangedFrozen;
    private Object[] originalMetadata;

	// Constructors

	public AT(Repository repository, ATData atData, ATStateData atStateData) {
		this.repository = repository;
		this.atData = atData;
		this.atStateData = atStateData;
        this.originalMetadata = metadata();
	}

	public AT(Repository repository, ATData atData) {
		this(repository, atData, null);
	}

	/** Constructs AT-handling object when deploying AT */
	public AT(Repository repository, DeployAtTransactionData deployATTransactionData) throws DataException {
		this.repository = repository;

		String atAddress = deployATTransactionData.getAtAddress();
		int height = this.repository.getBlockRepository().getBlockchainHeight() + 1;
		byte[] creatorPublicKey = deployATTransactionData.getCreatorPublicKey();
		long creation = deployATTransactionData.getTimestamp();
		long assetId = deployATTransactionData.getAssetId();

		// Just enough AT data to allow API to query initial balances, etc.
		ATData skeletonAtData = new ATData(atAddress, creatorPublicKey, creation, assetId);

		long blockTimestamp = Timestamp.toLong(height, 0);
		QortalATAPI api = new QortalATAPI(repository, skeletonAtData, blockTimestamp);
		QortalAtLoggerFactory loggerFactory = QortalAtLoggerFactory.getInstance();

		MachineState machineState = new MachineState(api, loggerFactory, deployATTransactionData.getCreationBytes());

		byte[] codeBytes = machineState.getCodeBytes();
		byte[] codeHash = Crypto.digest(codeBytes);

		this.atData = new ATData(atAddress, creatorPublicKey, creation, machineState.version, assetId, codeBytes, codeHash,
				machineState.isSleeping(), machineState.getSleepUntilHeight(), machineState.isFinished(), machineState.hadFatalError(),
				machineState.isFrozen(), machineState.getFrozenBalance(), null);

		byte[] stateData = machineState.toBytes();
		byte[] stateHash = Crypto.digest(stateData);

		this.atStateData = new ATStateData(atAddress, height, stateData, stateHash, 0L, true, null);
	}

	// Getters / setters

	public ATStateData getATStateData() {
		return this.atStateData;
	}

	// Processing

	public void deploy() throws DataException {
		ATRepository atRepository = this.repository.getATRepository();
		atRepository.save(this.atData);

		atRepository.saveInitialState(this.atStateData);

		// Record the initial state head so getExecutionStates() can find it
		atRepository.setCurrentStateHead(this.atData.getATAddress(), this.atStateData.getHeight(), this.atStateData.getStateHash());
	}

	public void undeploy() throws DataException {
		// AT states deleted implicitly by repository
		this.repository.getATRepository().delete(this.atData.getATAddress());
	}

	/**
	 * Potentially execute AT.
	 * <p>
	 * Note that sleep-until-message support might set/reset
	 * sleep-related flags/values.
	 * <p>
	 * {@link #getATStateData()} will return null if nothing happened.
	 * <p>
	 * @param blockHeight
	 * @param blockTimestamp
	 * @return AT-generated transactions, possibly empty
	 * @throws DataException
	 */
	public List<AtTransaction> run(int blockHeight, long blockTimestamp) throws DataException {
        return run(blockHeight, blockTimestamp, null);
    }

    public List<AtTransaction> run(int blockHeight, long blockTimestamp, ATExecutionContext context) throws DataException {
		String atAddress = this.atData.getATAddress();

		QortalATAPI api = new QortalATAPI(repository, this.atData, blockTimestamp, context);
		QortalAtLoggerFactory loggerFactory = QortalAtLoggerFactory.getInstance();

		if (!api.willExecute(blockHeight))
			// this.atStateData will be null
			return Collections.emptyList();

        org.qortal.data.at.ATExecutionState parent = context == null ? null : context.state(atAddress);
        if (context != null && parent == null) throw new DataException("Missing AT parent head");
        // An unchanged frozen receipt references the proven parent payload without loading its bytes.
        if (context != null && this.atData.getIsFrozen() && this.atData.getFrozenBalance() != null
                && this.atData.getSleepUntilMessageTimestamp() == null && context.balance(atAddress) <= this.atData.getFrozenBalance()) {
            this.unchangedFrozen = true;
            this.atStateData = new org.qortal.data.at.ATStateReference(parent, blockHeight, 0L, null);
            return Collections.emptyList();
        }
        ATStateData latestAtStateData = context == null ? repository.getATRepository().getLatestATState(atAddress) : null;
        byte[] previousBytes = context == null ? (latestAtStateData == null ? null : latestAtStateData.getStateData())
                : repository.getATRepository().getStatePayload(parent.getAddress(), parent.getHeight(), parent.getHash());
        byte[] previousHash = context == null ? (latestAtStateData == null ? null : latestAtStateData.getStateHash()) : parent.getHash();
        if (previousBytes == null) throw new DataException("No previous AT state data found");

		// [Re]create AT machine state using AT state data or from scratch as applicable
		byte[] codeBytes = this.atData.getCodeBytes();
        if (codeBytes == null) codeBytes = repository.getATRepository().getCodePayload(this.atData.getCodeHash());
        MachineState state = MachineState.fromBytes(api, loggerFactory, previousBytes, codeBytes);
		try {
			api.preExecute(state);
			state.execute();
		} catch (Exception e) {
			throw new DataException(String.format("Uncaught exception while running AT '%s'", atAddress), e);
		}

		this.resultingState = state;
		byte[] stateData = state.toBytes();
		byte[] stateHash = Crypto.digest(stateData);

		// Nothing happened?
		if (state.getSteps() == 0 && Arrays.equals(stateHash, previousHash))
			// We currently want to execute frozen ATs, to maintain backwards support.
			if (!state.isFrozen())
				// this.atStateData will be null
				return Collections.emptyList();

		long atFees = api.calcFinalFees(state);
		Long sleepUntilMessageTimestamp = this.atData.getSleepUntilMessageTimestamp();

		this.atStateData = context != null && Arrays.equals(stateData, previousBytes)
                ? new org.qortal.data.at.ATStateReference(parent, blockHeight, atFees, sleepUntilMessageTimestamp)
                : new ATStateData(atAddress, blockHeight, stateData, stateHash, atFees, false, sleepUntilMessageTimestamp);

		return api.getTransactions();
	}

    private Object[] metadata() {
        return new Object[]{atData.getIsSleeping(), atData.getSleepUntilHeight(), atData.getIsFinished(),
                atData.getHadFatalError(), atData.getIsFrozen(), atData.getFrozenBalance(), atData.getSleepUntilMessageTimestamp()};
    }

    public boolean hasChangedMetadata() { return !Arrays.equals(originalMetadata, metadata()); }

    /** Reuse the validated VM flags; no repository read and no second state decode. */
    public ATData prepareUpdate() {
        if (unchangedFrozen) return this.atData;
        MachineState state = resultingState != null ? resultingState : MachineState.flagsOnlyfromBytes(this.atStateData.getStateData());
        this.atData.setIsSleeping(state.isSleeping());
        this.atData.setSleepUntilHeight(state.getSleepUntilHeight());
        this.atData.setIsFinished(state.isFinished());
        this.atData.setHadFatalError(state.hadFatalError());
        this.atData.setIsFrozen(state.isFrozen());
        this.atData.setFrozenBalance(state.getFrozenBalance());
        this.atData.setSleepUntilMessageTimestamp(this.atStateData.getSleepUntilMessageTimestamp());
        return this.atData;
    }

    /** Restore the exact parent VM flags without executing the VM or writing immutable definitions. */
    public static void restoreMetadata(ATData atData, ATStateData previousStateData) throws DataException {
        if (previousStateData == null || previousStateData.getStateData() == null)
            throw new DataException("Can't find previous AT state data for " + atData.getATAddress());
		// Extract minimal/flags-only AT machine state using AT state data
		MachineState state = MachineState.flagsOnlyfromBytes(previousStateData.getStateData());

		// Update AT info in repository
		atData.setIsSleeping(state.isSleeping());
		atData.setSleepUntilHeight(state.getSleepUntilHeight());
		atData.setIsFinished(state.isFinished());
		atData.setHadFatalError(state.hadFatalError());
		atData.setIsFrozen(state.isFrozen());
		atData.setFrozenBalance(state.getFrozenBalance());

		// Special sleep-until-message support
		atData.setSleepUntilMessageTimestamp(previousStateData.getSleepUntilMessageTimestamp());
	}

}
