package org.qortal.at;

import org.qortal.data.at.ATData;
import org.qortal.data.at.ATExecutionState;
import org.qortal.data.transaction.TransactionData;
import org.qortal.repository.*;

import java.util.*;
import java.util.stream.Collectors;

/** Immutable parent-view inputs plus memoized reads, scoped to one sequential execution only. */
public final class ATExecutionContext {
    private final Repository repository;
    private final int parentHeight;
    private final org.qortal.data.block.BlockData parent;
    private byte[] previousSignature;
    private final Map<String, ATExecutionState> states = new HashMap<>();
    private final Map<String, Long> balances = new HashMap<>();
    private final Map<String, Integer> creationHeights = bounded();
    private final Map<String, byte[]> references = bounded();
    private final Map<String, Integer> levels = bounded();
    private final Map<String, Integer> blocksMinted = bounded();
    private static final long TRANSACTION_CACHE_BYTES = 4L * 1024 * 1024;
    private static final class TransactionRead {
        final TransactionData data;
        final long bytes;
        TransactionRead(TransactionData data, long bytes) { this.data = data; this.bytes = bytes; }
    }
    private final Map<Long, TransactionRead> transactions = new LinkedHashMap<>(256,0.75f,true);
    private long transactionBytes;
    private final Map<String, ATRepository.NextTransactionInfo> nextTransactions = bounded();

    private static <K,V> Map<K,V> bounded() {
        return new LinkedHashMap<K,V>(256, 0.75f, true) {
            @Override protected boolean removeEldestEntry(Map.Entry<K,V> entry) { return size() > 4096; }
        };
    }

    public ATExecutionContext(Repository repository, List<ATData> ats) throws DataException {
        this.repository = repository;
        this.parent = repository.getBlockRepository().getLastBlock();
        this.parentHeight = parent.getHeight();
        List<String> addresses = ats.stream().map(ATData::getATAddress).collect(Collectors.toList());
        for (ATExecutionState state : repository.getATRepository().getExecutionStates(addresses)) {
            states.put(state.getAddress(), state);
            balances.put(state.getAddress(), state.getBalance());
        }
    }

    public byte[] previousSignature() throws DataException {
        if (previousSignature == null) {
            List<org.qortal.data.block.BlockSummaryData> summaries = repository.getBlockRepository().getBlockSummaries(parentHeight-1,parentHeight-1);
            if (summaries == null || summaries.size() != 1) throw new DataException("AT previous block summary is unavailable");
            previousSignature = summaries.get(0).getSignature().clone();
        }
        return previousSignature.clone();
    }

    public byte[] parentSignature() { return parent.getSignature().clone(); }
    public org.qortal.data.block.BlockData parent() { return parent; }
    public int height() { return parentHeight; }
    public ATExecutionState state(String address) { return states.get(address); }
    public long balance(String address) { return balances.getOrDefault(address, 0L); }
    public Integer creationHeight(String address) throws DataException {
        if (!creationHeights.containsKey(address)) creationHeights.put(address, repository.getATRepository().getATCreationBlockHeight(address));
        return creationHeights.get(address);
    }
    public byte[] reference(String address) throws DataException {
        if (!references.containsKey(address)) references.put(address, repository.getAccountRepository().getLastReference(address));
        return references.get(address);
    }
    public Integer level(String address) throws DataException {
        if (!levels.containsKey(address)) levels.put(address, repository.getAccountRepository().getLevel(address));
        return levels.get(address);
    }
    public Integer blocksMinted(String address) throws DataException {
        if (!blocksMinted.containsKey(address)) blocksMinted.put(address, repository.getAccountRepository().getMintedBlockCount(address));
        return blocksMinted.get(address);
    }
    public TransactionData transaction(int height, int sequence) throws DataException {
        long key = ((long)height << 32) | (sequence & 0xffffffffL);
        TransactionRead cached = transactions.get(key);
        if (cached != null) return cached.data;
        TransactionData data = repository.getTransactionRepository().fromHeightAndSequence(height, sequence);
        long bytes;
        try { bytes = data == null ? 64 : 512L + 2L * org.qortal.transform.transaction.TransactionTransformer.getDataLength(data); }
        catch (org.qortal.transform.TransformationException e) { return data; } // Optional cache must not change VM behavior.
        if (bytes <= TRANSACTION_CACHE_BYTES) {
            Iterator<TransactionRead> oldest = transactions.values().iterator();
            while ((transactionBytes + bytes > TRANSACTION_CACHE_BYTES || transactions.size() >= 4096) && oldest.hasNext()) {
                transactionBytes -= oldest.next().bytes; oldest.remove();
            }
            transactions.put(key,new TransactionRead(data,bytes)); transactionBytes += bytes;
        }
        return data;
    }
    public ATRepository.NextTransactionInfo next(String address, int height, int sequence) throws DataException {
        String key = address + ':' + height + ':' + sequence;
        if (!nextTransactions.containsKey(key)) nextTransactions.put(key, repository.getATRepository().findNextTransaction(address, height, sequence));
        return nextTransactions.get(key);
    }
}
