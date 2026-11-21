package org.qortal.at;

import org.qortal.data.at.ATData;
import org.qortal.data.at.ATExecutionState;
import org.qortal.data.at.ATStateData;
import org.qortal.repository.DataException;
import org.qortal.repository.Repository;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.qortal.crypto.Crypto;

/** Validated outputs owned by one candidate. No mutable VM survives execution. */
public final class ATBlockResult {
	private static final int MAGIC = 0x51415442; // QATB
	private static final short FORMAT_VERSION = 1;
	private static final int HASH_LENGTH = 32;
	private static final int MAX_ENTRY_COUNT = 100_000;
	// Fits the inline VARBINARY(16M) repository column including the envelope.
	private static final int MAX_BODY_LENGTH = 15 * 1024 * 1024;

    private final byte[] parentSignature;
    private final int height;
    private final byte[] blob;
    private final List<ATData> updatedHeads;

    public ATBlockResult(byte[] parentSignature, int height, List<Entry> entries, List<ATData> updatedHeads) throws DataException {
        this.parentSignature = parentSignature.clone();
        this.height = height;
        this.blob = serialize(entries);
        this.updatedHeads = updatedHeads;
    }

    public void requireParent(Repository repository, int candidateHeight) throws DataException {
        if (height != candidateHeight || !Arrays.equals(parentSignature, repository.getBlockRepository().getLastBlock().getSignature()))
            throw new DataException("AT execution result belongs to a different parent block");
    }

    public void save(Repository repository, List<ATStateData> states) throws DataException {
        repository.getATRepository().saveBlockResult(height, blob, updatedHeads, states);
    }

    public static byte[] serialize(List<Entry> entries) throws DataException {
		if (entries == null || entries.size() > MAX_ENTRY_COUNT)
			throw new DataException("Invalid AT block result entry count");

        try {
			ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream(4096);
			DataOutputStream dos = new DataOutputStream(bodyBytes);
            dos.writeInt(entries.size());
            for (Entry e : entries) {
				if (e == null || e.address == null || e.address.isEmpty() || e.address.length() > 64
						|| e.stateHash == null || e.stateHash.length != HASH_LENGTH
						|| (!e.isInitial && (e.prevHeight < 0 || e.prevStateHash == null || e.prevStateHash.length != HASH_LENGTH)))
					throw new DataException("Invalid AT block result entry");
                dos.writeUTF(e.address);
                dos.write(e.stateHash);
                dos.writeLong(e.fees);
                dos.writeBoolean(e.isInitial);
                dos.writeBoolean(e.sleepUntilMessageTimestamp != null);
                if (e.sleepUntilMessageTimestamp != null)
                    dos.writeLong(e.sleepUntilMessageTimestamp);
                if (!e.isInitial) {
                    dos.writeInt(e.prevHeight);
                    dos.write(e.prevStateHash);
                    dos.writeBoolean(e.prevIsSleeping);
                    dos.writeBoolean(e.prevSleepUntilHeight != null);
                    if (e.prevSleepUntilHeight != null)
                        dos.writeInt(e.prevSleepUntilHeight);
                    dos.writeBoolean(e.prevIsFinished);
                    dos.writeBoolean(e.prevHadFatalError);
                    dos.writeBoolean(e.prevIsFrozen);
                    dos.writeBoolean(e.prevFrozenBalance != null);
                    if (e.prevFrozenBalance != null)
                        dos.writeLong(e.prevFrozenBalance);
                    dos.writeBoolean(e.prevSleepUntilMessageTimestamp != null);
                    if (e.prevSleepUntilMessageTimestamp != null)
                        dos.writeLong(e.prevSleepUntilMessageTimestamp);
                }
            }
            dos.flush();
			byte[] body = bodyBytes.toByteArray();
			if (body.length > MAX_BODY_LENGTH)
				throw new DataException("AT block result exceeds maximum size");

			ByteArrayOutputStream envelopeBytes = new ByteArrayOutputStream(body.length + 42);
			DataOutputStream envelope = new DataOutputStream(envelopeBytes);
			envelope.writeInt(MAGIC);
			envelope.writeShort(FORMAT_VERSION);
			envelope.writeInt(body.length);
			envelope.write(body);
			envelope.write(Crypto.digest(body));
			envelope.flush();
			return envelopeBytes.toByteArray();
        } catch (IOException ex) {
            throw new DataException("Unable to serialize AT block result", ex);
        }
    }

    public static List<Entry> deserialize(byte[] blob) throws DataException {
		if (blob == null || blob.length < 4 + 2 + 4 + HASH_LENGTH)
			throw new DataException("AT block result is truncated");

        try {
			DataInputStream envelope = new DataInputStream(new ByteArrayInputStream(blob));
			if (envelope.readInt() != MAGIC)
				throw new DataException("Unknown AT block result format");
			short version = envelope.readShort();
			if (version != FORMAT_VERSION)
				throw new DataException("Unsupported AT block result version: " + version);
			int bodyLength = envelope.readInt();
			if (bodyLength < 4 || bodyLength > MAX_BODY_LENGTH || bodyLength != envelope.available() - HASH_LENGTH)
				throw new DataException("Invalid AT block result length");
			byte[] body = new byte[bodyLength];
			envelope.readFully(body);
			byte[] checksum = new byte[HASH_LENGTH];
			envelope.readFully(checksum);
			if (envelope.available() != 0 || !Arrays.equals(checksum, Crypto.digest(body)))
				throw new DataException("AT block result checksum mismatch");

			DataInputStream dis = new DataInputStream(new ByteArrayInputStream(body));
            int count = dis.readInt();
			if (count < 0 || count > MAX_ENTRY_COUNT)
				throw new DataException("Invalid AT block result entry count");
            List<Entry> entries = new ArrayList<>(count);
            for (int i = 0; i < count; ++i) {
                String address = dis.readUTF();
				if (address.isEmpty() || address.length() > 64)
					throw new DataException("Invalid AT block result address");
                byte[] stateHash = new byte[32];
                dis.readFully(stateHash);
                long fees = dis.readLong();
                boolean isInitial = dis.readBoolean();
                Long sleepUntilMessageTimestamp = dis.readBoolean() ? dis.readLong() : null;
                int prevHeight = -1;
                byte[] prevStateHash = null;
                boolean prevIsSleeping = false;
                Integer prevSleepUntilHeight = null;
                boolean prevIsFinished = false;
                boolean prevHadFatalError = false;
                boolean prevIsFrozen = false;
                Long prevFrozenBalance = null;
                Long prevSleepUntilMessageTimestamp = null;
                if (!isInitial) {
                    prevHeight = dis.readInt();
                    prevStateHash = new byte[32];
                    dis.readFully(prevStateHash);
                    prevIsSleeping = dis.readBoolean();
                    prevSleepUntilHeight = dis.readBoolean() ? dis.readInt() : null;
                    prevIsFinished = dis.readBoolean();
                    prevHadFatalError = dis.readBoolean();
                    prevIsFrozen = dis.readBoolean();
                    prevFrozenBalance = dis.readBoolean() ? dis.readLong() : null;
                    prevSleepUntilMessageTimestamp = dis.readBoolean() ? dis.readLong() : null;
                }
                entries.add(new Entry(address, stateHash, fees, isInitial, sleepUntilMessageTimestamp,
                    prevHeight, prevStateHash, prevIsSleeping, prevSleepUntilHeight, prevIsFinished,
                    prevHadFatalError, prevIsFrozen, prevFrozenBalance, prevSleepUntilMessageTimestamp));
            }
			if (dis.available() != 0)
				throw new DataException("AT block result has trailing data");
            return Collections.unmodifiableList(entries);
        } catch (IOException ex) {
            throw new DataException("Unable to deserialize AT block result", ex);
        }
    }

	public static final class Entry {
        public final String address;
        public final byte[] stateHash;
        public final long fees;
        public final boolean isInitial;
        public final Long sleepUntilMessageTimestamp;
        public final int prevHeight;
        public final byte[] prevStateHash;
        public final boolean prevIsSleeping;
        public final Integer prevSleepUntilHeight;
        public final boolean prevIsFinished;
        public final boolean prevHadFatalError;
        public final boolean prevIsFrozen;
        public final Long prevFrozenBalance;
        public final Long prevSleepUntilMessageTimestamp;

        public Entry(String address, byte[] stateHash, long fees, boolean isInitial, Long sleepUntilMessageTimestamp,
            int prevHeight, byte[] prevStateHash, boolean prevIsSleeping, Integer prevSleepUntilHeight, boolean prevIsFinished,
            boolean prevHadFatalError, boolean prevIsFrozen, Long prevFrozenBalance, Long prevSleepUntilMessageTimestamp) {
            this.address = address;
            this.stateHash = stateHash.clone();
            this.fees = fees;
            this.isInitial = isInitial;
            this.sleepUntilMessageTimestamp = sleepUntilMessageTimestamp;
            this.prevHeight = prevHeight;
            this.prevStateHash = prevStateHash != null ? prevStateHash.clone() : null;
            this.prevIsSleeping = prevIsSleeping;
            this.prevSleepUntilHeight = prevSleepUntilHeight;
            this.prevIsFinished = prevIsFinished;
            this.prevHadFatalError = prevHadFatalError;
            this.prevIsFrozen = prevIsFrozen;
            this.prevFrozenBalance = prevFrozenBalance;
            this.prevSleepUntilMessageTimestamp = prevSleepUntilMessageTimestamp;
        }

		/** Convenience constructor from execution outputs and metadata captured before execution. */
		public Entry(ATStateData state, ParentMetadata parentHead, ATExecutionState parentState) {
			this(state.getATAddress(), state.getStateHash(), state.getFees(),
				state.isInitial(), state.getSleepUntilMessageTimestamp(),
				parentState != null && !state.isInitial() ? parentState.getHeight() : -1,
				parentState != null && !state.isInitial() ? parentState.getHash() : null,
				parentHead.isSleeping, parentHead.sleepUntilHeight,
				parentHead.isFinished, parentHead.hadFatalError,
				parentHead.isFrozen, parentHead.frozenBalance,
				parentHead.sleepUntilMessageTimestamp);
		}
	}

	/** Immutable mutable-head fields captured before AT execution can change them. */
	public static final class ParentMetadata {
		private final boolean isSleeping;
		private final Integer sleepUntilHeight;
		private final boolean isFinished;
		private final boolean hadFatalError;
		private final boolean isFrozen;
		private final Long frozenBalance;
		private final Long sleepUntilMessageTimestamp;

		public ParentMetadata(ATData data) {
			this.isSleeping = data.getIsSleeping();
			this.sleepUntilHeight = data.getSleepUntilHeight();
			this.isFinished = data.getIsFinished();
			this.hadFatalError = data.getHadFatalError();
			this.isFrozen = data.getIsFrozen();
			this.frozenBalance = data.getFrozenBalance();
			this.sleepUntilMessageTimestamp = data.getSleepUntilMessageTimestamp();
		}
	}
}
