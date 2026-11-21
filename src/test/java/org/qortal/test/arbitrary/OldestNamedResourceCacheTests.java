package org.qortal.test.arbitrary;

import org.junit.Before;
import org.junit.Test;
import org.qortal.arbitrary.misc.Service;
import org.qortal.controller.arbitrary.ArbitraryDataCacheManager;
import org.qortal.data.arbitrary.ArbitraryResourceData;
import org.qortal.data.transaction.ArbitraryTransactionData;
import org.qortal.data.transaction.BaseTransactionData;
import org.qortal.group.Group;
import org.qortal.repository.Repository;
import org.qortal.repository.RepositoryManager;
import org.qortal.test.common.Common;
import org.qortal.transaction.Transaction;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class OldestNamedResourceCacheTests extends Common {

    @Before
    public void beforeTest() throws Exception {
        Common.useDefaultSettings();
    }

    private ArbitraryTransactionData insert(Repository repository, int sequence, long timestamp,
                                            String name, String identifier) throws Exception {
        byte[] signature = new byte[64];
        ByteBuffer.wrap(signature).putInt(0x51444e00).putInt(sequence);
        BaseTransactionData base = new BaseTransactionData(timestamp, Group.NO_GROUP, new byte[64],
                new byte[32], 0L, Transaction.ApprovalStatus.NOT_REQUIRED, null, null, signature);
        ArbitraryTransactionData tx = new ArbitraryTransactionData(base, 5, Service.WEBSITE.value, 0, 1,
                name, identifier, ArbitraryTransactionData.Method.PATCH, null,
                ArbitraryTransactionData.Compression.NONE, new byte[]{1},
                ArbitraryTransactionData.DataType.RAW_DATA, null, Collections.emptyList());
        repository.getTransactionRepository().save(tx);
        return tx;
    }

    private ArbitraryResourceData cache(Repository repository, Service service, String name,
                                        String identifier) throws Exception {
        ArbitraryResourceData resource = new ArbitraryResourceData(service, name, identifier);
        resource.created = 1L;
        resource.size = 1;
        repository.getArbitraryRepository().save(resource);
        return resource;
    }

    private void assertNeedsRebuild(Repository repository, boolean expected) throws Exception {
        assertEquals(expected, ArbitraryDataCacheManager.getInstance().needsArbitraryResourcesCacheRebuild(repository));
    }

    private void assertMatchesLegacy(Repository repository) throws Exception {
        List<ArbitraryTransactionData> rows = repository.getArbitraryRepository()
                .getArbitraryTransactions(true, 1, 0, false);
        ArbitraryTransactionData oldest = rows.get(0);
        assertNeedsRebuild(repository, repository.getArbitraryRepository()
                .getArbitraryResource(oldest.getService(), oldest.getName(), oldest.getIdentifier()) == null);
    }

    @Test
    public void checksOnlyTheOldestNamedKeyAndObservesSubsequentChanges() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            assertNeedsRebuild(repository, false);
            insert(repository, 1, 10, null, null);
            assertNeedsRebuild(repository, false);

            insert(repository, 2, 30, "later", null);
            cache(repository, Service.WEBSITE, "later", null);
            ArbitraryTransactionData oldest = insert(repository, 3, 20, "First", null);
            cache(repository, Service.WEBSITE, "first", "another-id");
            cache(repository, Service.BLOG, "first", null);
            assertNeedsRebuild(repository, true);

            ArbitraryResourceData first = cache(repository, Service.WEBSITE, "first", "default");
            assertNeedsRebuild(repository, false);
            repository.getArbitraryRepository().delete(first);
            assertNeedsRebuild(repository, true);
            repository.getTransactionRepository().delete(oldest);
            repository.saveChanges();
            assertNeedsRebuild(repository, false);
        }
    }

    @Test
    public void preservesLegacyDecisionForEveryCacheStateOfTiedKeys() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            insert(repository, 1, 10, "shared", "b");
            insert(repository, 2, 10, "shared", "a");
            insert(repository, 3, 10, "shared", "a");
            insert(repository, 4, 10, null, null);
            assertNeedsRebuild(repository, true);

            ArbitraryResourceData a = cache(repository, Service.WEBSITE, "shared", "a");
            assertMatchesLegacy(repository);
            cache(repository, Service.WEBSITE, "shared", "b");
            assertNeedsRebuild(repository, false);
            repository.getArbitraryRepository().delete(a);
            repository.saveChanges();
            assertMatchesLegacy(repository);
        }
    }

    @Test
    public void historicalEmptyIdentifierUsesTheSameLookupAsTransactionDecoding() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            insert(repository, 1, 10, "old", new String(new char[0]));
            assertMatchesLegacy(repository);
            cache(repository, Service.WEBSITE, "old", "default");
            assertMatchesLegacy(repository);
            cache(repository, Service.WEBSITE, "old", "");
            repository.saveChanges();
            assertMatchesLegacy(repository);
        }
    }
}
