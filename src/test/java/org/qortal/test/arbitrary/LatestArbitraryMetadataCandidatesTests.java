package org.qortal.test.arbitrary;

import org.junit.Before;
import org.junit.Test;
import org.qortal.arbitrary.misc.Service;
import org.qortal.controller.arbitrary.ArbitraryTransactionDataHashWrapper;
import org.qortal.data.transaction.ArbitraryTransactionData;
import org.qortal.data.transaction.BaseTransactionData;
import org.qortal.group.Group;
import org.qortal.repository.Repository;
import org.qortal.repository.RepositoryManager;
import org.qortal.test.common.Common;
import org.qortal.transaction.Transaction;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class LatestArbitraryMetadataCandidatesTests extends Common {

    @Before
    public void beforeTest() throws Exception {
        Common.useDefaultSettings();
    }

    private ArbitraryTransactionData transaction(int sequence, long timestamp, String name) {
        byte[] signature = new byte[64];
        ByteBuffer.wrap(signature).putInt(0x51444e00).putInt(sequence);
        byte[] metadataHash = new byte[32];
        ByteBuffer.wrap(metadataHash).putInt(sequence);
        BaseTransactionData base = new BaseTransactionData(timestamp, Group.NO_GROUP,
                new byte[64], new byte[32], 0L, Transaction.ApprovalStatus.NOT_REQUIRED,
                null, null, signature);
        return new ArbitraryTransactionData(base, 5, Service.WEBSITE.value, 0, 1,
                name, sequence % 3 == 0 ? null : "id-" + sequence,
                ArbitraryTransactionData.Method.PUT, null,
                ArbitraryTransactionData.Compression.NONE, new byte[] { 1 },
                ArbitraryTransactionData.DataType.RAW_DATA, metadataHash, Collections.emptyList());
    }

    private void assertMatchesLegacyQuery(Repository repository, int limit) throws Exception {
        List<ArbitraryTransactionData> legacy = repository.getArbitraryRepository()
                .getLatestArbitraryTransactions(limit);
        List<ArbitraryTransactionDataHashWrapper> lightweight = repository.getArbitraryRepository()
                .getLatestArbitraryMetadataCandidates(limit);

        assertEquals(legacy.size(), lightweight.size());
        for (int i = 0; i < legacy.size(); ++i) {
            ArbitraryTransactionData expected = legacy.get(i);
            ArbitraryTransactionDataHashWrapper actual = lightweight.get(i);
            assertArrayEquals(expected.getSignature(), actual.getSignature());
            assertEquals(expected.getService().value, actual.getService());
            assertEquals(expected.getName(), actual.getName());
            assertEquals(expected.getIdentifier(), actual.getIdentifier());
            assertArrayEquals(expected.getMetadataHash(), actual.getMetadataHash());
            assertEquals(expected.getTimestamp(), actual.getTimestamp());
        }
    }

    @Test
    public void lightweightQueryMatchesLegacyResultsAcrossInsertAndDelete() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            long timestamp = System.currentTimeMillis() + 1_000_000L;
            List<ArbitraryTransactionData> inserted = new ArrayList<>();
            for (int i = 0; i < 125; ++i) {
                ArbitraryTransactionData transaction = transaction(i, timestamp + i,
                        i % 11 == 0 ? null : "resource-" + (i % 17));
                repository.getTransactionRepository().save(transaction);
                inserted.add(transaction);
            }
            repository.saveChanges();

            assertMatchesLegacyQuery(repository, 100);

            repository.getTransactionRepository().delete(inserted.get(inserted.size() - 1));
            repository.saveChanges();
            assertMatchesLegacyQuery(repository, 100);
        }
    }
}
