package org.qortal.test.arbitrary;

import org.junit.Before;
import org.junit.Test;
import org.qortal.account.PrivateKeyAccount;
import org.qortal.arbitrary.ArbitraryDataDigest;
import org.qortal.arbitrary.ArbitraryDataBuildQueueItem;
import org.qortal.arbitrary.ArbitraryDataFile;
import org.qortal.arbitrary.ArbitraryDataFile.ResourceIdType;
import org.qortal.arbitrary.ArbitraryDataReader;
import org.qortal.arbitrary.misc.Service;
import org.qortal.data.arbitrary.ArbitraryResourceData;
import org.qortal.data.transaction.ArbitraryTransactionData;
import org.qortal.data.transaction.BaseTransactionData;
import org.qortal.data.transaction.RegisterNameTransactionData;
import org.qortal.group.Group;
import org.qortal.repository.Repository;
import org.qortal.repository.RepositoryManager;
import org.qortal.test.common.Common;
import org.qortal.test.common.ArbitraryUtils;
import org.qortal.test.common.TransactionUtils;
import org.qortal.test.common.transaction.TestTransaction;
import org.qortal.transaction.RegisterNameTransaction;
import org.qortal.transaction.Transaction;
import org.qortal.utils.Base58;
import org.qortal.utils.ArbitraryTransactionUtils;
import org.qortal.utils.NTP;

import java.util.Collections;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.*;
import static org.qortal.data.arbitrary.ArbitraryResourceStatus.Status.READY;

public class ArbitraryBuildSchedulingMetadataTests extends Common {
    @Before
    public void setUp() throws Exception {
        Common.useDefaultSettings();
    }

    @Test
    public void publishedSizeClassifiesLargeBuildBeforeFilesAreDownloaded() throws Exception {
        String name = "scheduling-size";
        byte[] signature = new byte[64];
        signature[0] = 73;
        int size = 101 * 1024 * 1024;
        BaseTransactionData base = new BaseTransactionData(NTP.getTime(), Group.NO_GROUP,
                new byte[64], new byte[32], 0L, Transaction.ApprovalStatus.NOT_REQUIRED,
                null, null, signature);
        ArbitraryTransactionData transaction = new ArbitraryTransactionData(base, 5, Service.WEBSITE.value,
                0, size, name, null, ArbitraryTransactionData.Method.PUT, null,
                ArbitraryTransactionData.Compression.ZIP, new byte[32],
                ArbitraryTransactionData.DataType.DATA_HASH, null, Collections.emptyList());
        try (Repository repository = RepositoryManager.getRepository()) {
            repository.getTransactionRepository().save(transaction);
            ArbitraryResourceData resource = new ArbitraryResourceData(Service.WEBSITE, name, null);
            resource.latestSignature = signature;
            resource.size = size;
            resource.created = NTP.getTime();
            repository.getArbitraryRepository().save(resource);
            repository.saveChanges();
        }

        ArbitraryDataBuildQueueItem item = new ArbitraryDataBuildQueueItem(name, ResourceIdType.NAME, Service.WEBSITE, null);
        item.setPriority(10);
        assertFalse(item.isLargeFile());
        item.initializeSchedulingMetadata();
        assertTrue(item.isLargeFile());
        assertEquals(10, item.getPriority());
        assertEquals(5L, item.getEffectivePriority(NTP.getTime()));
        assertNull("Scheduling must not require a disk chunk scan", item.getTotalChunkCount());
    }

    @Test
    public void namedDirectoryBuildPublishesCompleteOutputAndKeepsSharedArchive() throws Exception {
        String name = "builder test";
        Service service = Service.ARBITRARY_DATA;
        Path input = Paths.get("src/test/resources/arbitrary/demo1");
        ArbitraryDataFile archive;
        try (Repository repository = RepositoryManager.getRepository()) {
            PrivateKeyAccount alice = Common.getTestAccount(repository, "alice");
            RegisterNameTransactionData registration = new RegisterNameTransactionData(
                    TestTransaction.generateBase(alice), name, "");
            registration.setFee(new RegisterNameTransaction(null, null).getUnitFee(registration.getTimestamp()));
            TransactionUtils.signAndMint(repository, registration, alice);
            archive = ArbitraryUtils.createAndMintTxn(repository, Base58.encode(alice.getPublicKey()), input,
                    name, null, ArbitraryTransactionData.Method.PUT, service, alice);
        }

        ArbitraryDataReader reader = new ArbitraryDataReader(name, ResourceIdType.NAME, service, null);
        assertTrue(ArbitraryTransactionUtils.getStatus(service, name, null, false, false).getStatus() != READY);
        reader.loadSynchronously(true);
        assertEquals(READY,
                ArbitraryTransactionUtils.getStatus(service, name, null, false, false).getStatus());
        ArbitraryDataDigest expected = new ArbitraryDataDigest(input);
        ArbitraryDataDigest actual = new ArbitraryDataDigest(reader.getFilePath());
        expected.compute();
        actual.compute();
        assertArrayEquals(expected.getHash(), actual.getHash());
        assertTrue("The encrypted archive must remain available to peers", archive.exists());

        ArbitraryDataReader transactionReader = new ArbitraryDataReader(Base58.encode(reader.getLatestSignature()),
                ResourceIdType.TRANSACTION_DATA, service, null);
        assertFalse("The extracted staging directory was moved, not retained as another copy",
                Files.exists(transactionReader.getUncompressedPath()));
        ArbitraryDataReader cached = new ArbitraryDataReader(name, ResourceIdType.NAME, service, null);
        cached.loadSynchronously(false);
        assertEquals(reader.getFilePath(), cached.getFilePath());
    }
}
