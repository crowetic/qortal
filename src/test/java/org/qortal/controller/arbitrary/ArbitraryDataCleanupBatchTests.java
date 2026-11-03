package org.qortal.controller.arbitrary;

import org.junit.Test;
import org.qortal.data.transaction.ArbitraryTransactionData;
import org.qortal.data.transaction.BaseTransactionData;
import org.qortal.repository.ArbitraryRepository;
import org.qortal.repository.DataException;

import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.*;

public class ArbitraryDataCleanupBatchTests {
    private static final long REFRESH_INTERVAL = 6 * 60 * 60 * 1000L;

    private static final class Fixture {
        final List<byte[]> signatures = new ArrayList<>();
        final List<List<byte[]>> requests = new ArrayList<>();
        final Set<Integer> missing = new HashSet<>();
        int snapshots;
        boolean failSnapshot;
        boolean failHydration;
        final ArbitraryRepository repository = (ArbitraryRepository) Proxy.newProxyInstance(
                ArbitraryRepository.class.getClassLoader(), new Class<?>[] {ArbitraryRepository.class}, (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "getArbitraryTransactionSignatures":
                            snapshots++;
                            if (failSnapshot) throw new DataException("snapshot failed");
                            return new ArrayList<>(signatures);
                        case "getArbitraryTransactionsBySignatures":
                            @SuppressWarnings("unchecked") List<byte[]> batch = (List<byte[]>) args[0];
                            requests.add(new ArrayList<>(batch));
                            if (failHydration) throw new DataException("hydration failed");
                            List<ArbitraryTransactionData> transactions = new ArrayList<>();
                            for (byte[] signature : batch) {
                                int id = ByteBuffer.wrap(signature).getInt();
                                if (!missing.contains(id)) transactions.add(transaction(signature, id));
                            }
                            return transactions;
                        default:
                            throw new AssertionError("Cleanup must not use " + method.getName());
                    }
                });
        Fixture(int size) {
            for (int id = size; id > 0; id--) signatures.add(ByteBuffer.allocate(64).putInt(id).array());
        }
    }

    private static ArbitraryTransactionData transaction(byte[] signature, int id) {
        BaseTransactionData base = new BaseTransactionData(id, 0, new byte[64], new byte[32], 0L, signature);
        return new ArbitraryTransactionData(base, 5, 100, 0, 32, "same", null,
                id % 2 == 0 ? ArbitraryTransactionData.Method.PUT : ArbitraryTransactionData.Method.PATCH,
                null, ArbitraryTransactionData.Compression.NONE, new byte[32],
                ArbitraryTransactionData.DataType.DATA_HASH, null, null);
    }

    @Test
    public void sweepsEveryVersionInBoundedNewestFirstBatches() throws Exception {
        Fixture fixture = new Fixture(205);
        ArbitraryDataCleanupManager.TransactionBatch cursor = new ArbitraryDataCleanupManager.TransactionBatch();
        List<ArbitraryTransactionData> all = new ArrayList<>();
        all.addAll(cursor.next(fixture.repository, 0)); assertTrue(cursor.startOfCycle);
        all.addAll(cursor.next(fixture.repository, 1)); assertFalse(cursor.startOfCycle);
        all.addAll(cursor.next(fixture.repository, 2)); assertFalse(cursor.startOfCycle);
        assertEquals(205, all.size());
        for (int index = 0; index < all.size(); index++) assertEquals(205 - index, all.get(index).getTimestamp());
        assertEquals(1, fixture.snapshots);
        assertEquals(100, fixture.requests.get(0).size());
        assertEquals(100, fixture.requests.get(1).size());
        assertEquals(5, fixture.requests.get(2).size());
        cursor.next(fixture.repository, 3); assertTrue(cursor.startOfCycle);
        assertEquals(1, fixture.snapshots);
    }

    @Test
    public void entirelyMissingBatchDoesNotRestartOrSkipTheFollowingPage() throws Exception {
        Fixture fixture = new Fixture(205);
        for (int id = 205; id > 105; id--) fixture.missing.add(id);
        ArbitraryDataCleanupManager.TransactionBatch cursor = new ArbitraryDataCleanupManager.TransactionBatch();
        assertTrue(cursor.next(fixture.repository, 0).isEmpty());
        List<ArbitraryTransactionData> second = cursor.next(fixture.repository, 1);
        assertFalse(cursor.startOfCycle);
        assertEquals(100, second.size());
        assertEquals(105, second.get(0).getTimestamp());
        assertEquals(1, fixture.snapshots);
    }

    @Test
    public void snapshotRefreshWaitsForSweepBoundaryAndIncludesNewTransactions() throws Exception {
        Fixture fixture = new Fixture(101);
        ArbitraryDataCleanupManager.TransactionBatch cursor = new ArbitraryDataCleanupManager.TransactionBatch();
        cursor.next(fixture.repository, 0);
        fixture.signatures.add(0, ByteBuffer.allocate(64).putInt(102).array());
        assertEquals(1, cursor.next(fixture.repository, REFRESH_INTERVAL).size());
        assertEquals(1, fixture.snapshots);
        assertEquals(102, cursor.next(fixture.repository, REFRESH_INTERVAL + 1).get(0).getTimestamp());
        assertTrue(cursor.startOfCycle);
        assertEquals(2, fixture.snapshots);
    }

    @Test
    public void hydrationFailureRetriesTheSamePage() throws Exception {
        Fixture fixture = new Fixture(205);
        ArbitraryDataCleanupManager.TransactionBatch cursor = new ArbitraryDataCleanupManager.TransactionBatch();
        cursor.next(fixture.repository, 0);
        fixture.failHydration = true;
        try {
            cursor.next(fixture.repository, 1);
            fail("Expected a repository failure");
        } catch (DataException expected) {
            // Expected: callers must be able to retry failed queries.
        }
        fixture.failHydration = false;
        assertEquals(105, cursor.next(fixture.repository, 2).get(0).getTimestamp());
        assertArrayEquals(fixture.requests.get(1).get(0), fixture.requests.get(2).get(0));
        assertEquals(1, fixture.snapshots);
    }

    @Test
    public void snapshotFailureRetriesWithoutWaitingSixHours() throws Exception {
        Fixture fixture = new Fixture(1);
        ArbitraryDataCleanupManager.TransactionBatch cursor = new ArbitraryDataCleanupManager.TransactionBatch();
        fixture.failSnapshot = true;
        try {
            cursor.next(fixture.repository, 0);
            fail("Expected a repository failure");
        } catch (DataException expected) {
            // Expected: callers must be able to retry failed queries.
        }
        fixture.failSnapshot = false;
        assertEquals(1, cursor.next(fixture.repository, 1).size());
        assertTrue(cursor.startOfCycle);
        assertEquals(2, fixture.snapshots);
    }

    @Test
    public void emptySnapshotIsReusedAndRefreshedOnSchedule() throws Exception {
        Fixture fixture = new Fixture(0);
        ArbitraryDataCleanupManager.TransactionBatch cursor = new ArbitraryDataCleanupManager.TransactionBatch();
        assertTrue(cursor.next(fixture.repository, 0).isEmpty());
        assertTrue(cursor.next(fixture.repository, 1).isEmpty());
        assertEquals(1, fixture.snapshots);
        fixture.signatures.add(ByteBuffer.allocate(64).putInt(1).array());
        assertEquals(1, cursor.next(fixture.repository, REFRESH_INTERVAL).size());
        assertEquals(2, fixture.snapshots);
    }
}
