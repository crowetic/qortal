package org.qortal.arbitrary;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.qortal.arbitrary.misc.Service;
import org.qortal.data.transaction.ArbitraryTransactionData;
import org.qortal.data.transaction.BaseTransactionData;
import org.qortal.group.Group;
import org.qortal.test.common.Common;
import org.qortal.transaction.Transaction;
import org.qortal.utils.Base58;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class QdnFileManifestCacheTests extends Common {

    @Before
    public void beforeTest() throws Exception {
        Common.useDefaultSettings();
    }

    @Test
    public void manifestCachesLayoutButObservesCurrentFilePresence() throws Exception {
        byte[] signature = filledBytes(64, (byte) 17);
        byte[] dataHash = filledBytes(32, (byte) 29);
        byte[] metadataHash = filledBytes(32, (byte) 43);
        byte[] firstChunkHash = filledBytes(32, (byte) 61);
        byte[] secondChunkHash = filledBytes(32, (byte) 79);

        Path metadataPath = QdnPathResolver.resolve(QdnPathResolver.encode(metadataHash), signature);
        Path dataPath = QdnPathResolver.resolve(QdnPathResolver.encode(dataHash), signature);
        Path firstChunkPath = QdnPathResolver.resolve(QdnPathResolver.encode(firstChunkHash), signature);
        Path secondChunkPath = QdnPathResolver.resolve(QdnPathResolver.encode(secondChunkHash), signature);
        Path transactionDirectory = dataPath.getParent();

        try {
            Files.createDirectories(transactionDirectory);
            String json = String.format("{\"chunks\":[\"%s\",\"%s\"]}",
                    Base58.encode(firstChunkHash), Base58.encode(secondChunkHash));
            Files.write(metadataPath, json.getBytes(StandardCharsets.UTF_8));

            ArbitraryTransactionData transaction = transaction(signature, dataHash, metadataHash);
            QdnFileManifestCache.Manifest manifest = QdnFileManifestCache.get(transaction);
            assertNotNull(manifest);
            assertEquals(2, manifest.chunkCount());
            assertEquals(3, manifest.expectedFileCount());
            assertFalse(manifest.allFilesExist());

            Files.write(firstChunkPath, new byte[] { 1 });
            assertFalse(manifest.allFilesExist());
            Files.write(secondChunkPath, new byte[] { 2 });
            assertTrue(manifest.allFilesExist());

            Files.delete(secondChunkPath);
            assertFalse(manifest.allFilesExist());
            Files.write(dataPath, new byte[] { 3 });
            assertTrue(manifest.allFilesExist());

            assertTrue("The same immutable manifest should be reused",
                    manifest == QdnFileManifestCache.get(transaction));
        } finally {
            FileUtils.deleteDirectory(transactionDirectory.toFile());
        }
    }

    private static ArbitraryTransactionData transaction(byte[] signature, byte[] dataHash, byte[] metadataHash) {
        BaseTransactionData base = new BaseTransactionData(System.currentTimeMillis(), Group.NO_GROUP,
                new byte[64], new byte[32], 0L, Transaction.ApprovalStatus.NOT_REQUIRED,
                null, null, signature);
        return new ArbitraryTransactionData(base, 5, Service.WEBSITE.value, 0, 1,
                "manifest-test", null, ArbitraryTransactionData.Method.PUT, null,
                ArbitraryTransactionData.Compression.NONE, dataHash,
                ArbitraryTransactionData.DataType.DATA_HASH, metadataHash, Collections.emptyList());
    }

    private static byte[] filledBytes(int size, byte value) {
        byte[] bytes = new byte[size];
        Arrays.fill(bytes, value);
        return bytes;
    }
}
