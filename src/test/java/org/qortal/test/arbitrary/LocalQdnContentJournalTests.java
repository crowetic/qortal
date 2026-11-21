package org.qortal.test.arbitrary;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.qortal.controller.arbitrary.ArbitraryDataBuildManager;
import org.qortal.controller.arbitrary.LocalQdnContentJournal;
import org.qortal.settings.Settings;
import org.qortal.test.common.Common;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class LocalQdnContentJournalTests extends Common {

    private Path dataPath;

    @Before
    public void beforeTest() throws Exception {
        Common.useDefaultSettings();
        this.dataPath = Paths.get(Settings.getInstance().getDataPath());
        FileUtils.deleteDirectory(this.dataPath.toFile());
        LocalQdnContentJournal.resetForTesting();
    }

    @After
    public void afterTest() throws Exception {
        LocalQdnContentJournal.resetForTesting();
        FileUtils.deleteDirectory(this.dataPath.toFile());
    }

    @Test
    public void journalSurvivesRestartAndCoalescesFilesystemChanges() {
        byte[] hosted = signature((byte) 1);
        byte[] changed = signature((byte) 2);

        LocalQdnContentJournal journal = LocalQdnContentJournal.getInstance();
        journal.recordFilesystemChange(hosted);
        journal.recordFilesystemChange(hosted);
        journal.confirmPresent(hosted);
        journal.markDiscoveryComplete();
        journal.recordFilesystemChange(changed);
        journal.compact();

        LocalQdnContentJournal.resetForTesting();
        journal = LocalQdnContentJournal.getInstance();
        assertTrue(journal.isDiscoveryComplete());
        assertEquals(1, journal.size());

        List<byte[]> changedCandidates = journal.nextCandidates(10, false);
        assertEquals(1, changedCandidates.size());
        assertArrayEquals(changed, changedCandidates.get(0));

        journal.confirmAbsent(changed);
        journal.confirmAbsent(hosted);
        assertEquals(0, journal.size());
    }

    @Test
    public void truncatedJournalRequiresSafeRediscovery() throws Exception {
        byte[] hosted = signature((byte) 3);
        LocalQdnContentJournal journal = LocalQdnContentJournal.getInstance();
        journal.confirmPresent(hosted);
        journal.markDiscoveryComplete();
        journal.compact();

        Path journalPath = this.dataPath.resolve(".qdn-content-journal-v1");
        Files.write(journalPath, new byte[] {9, 8, 7}, StandardOpenOption.APPEND);

        LocalQdnContentJournal.resetForTesting();
        journal = LocalQdnContentJournal.getInstance();
        assertFalse(journal.isDiscoveryComplete());
        assertEquals(1, journal.size());
        assertTrue(journal.nextCandidates(10, true).stream()
                .anyMatch(candidate -> Arrays.equals(candidate, hosted)));
    }

    @Test
    public void cleanupLeaseDefersWhileSignatureIsBeingWritten() throws Exception {
        byte[] signature = signature((byte) 4);
        LocalQdnContentJournal journal = LocalQdnContentJournal.getInstance();
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        Thread writer = new Thread(() -> {
            try (LocalQdnContentJournal.SignatureLease ignored = journal.acquireMutationLease(signature)) {
                locked.countDown();
                try {
                    release.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        writer.start();
        assertTrue(locked.await(5, TimeUnit.SECONDS));
        assertNull(journal.tryAcquireCleanupLease(signature));

        release.countDown();
        writer.join(5_000L);
        assertFalse(writer.isAlive());
        try (LocalQdnContentJournal.SignatureLease cleanup = journal.tryAcquireCleanupLease(signature)) {
            assertNotNull(cleanup);
        }
    }

    @Test
    public void discoveryBatchIsDurableAndResumesWithoutDuplicateRecords() throws Exception {
        LocalQdnContentJournal journal = LocalQdnContentJournal.getInstance();
        LinkedHashMap<String, byte[]> batch = new LinkedHashMap<>();
        for (byte value = 20; value < 30; ++value) {
            byte[] signature = signature(value);
            batch.put(org.qortal.utils.Base58.encode(signature), signature);
        }

        assertEquals(10, journal.confirmPresentBatch(batch));
        assertEquals(0, journal.confirmPresentBatch(batch));
        assertEquals(10, journal.size());

        LocalQdnContentJournal.resetForTesting();
        journal = LocalQdnContentJournal.getInstance();
        assertFalse(journal.isDiscoveryComplete());
        assertEquals(10, journal.size());
        for (byte[] signature : batch.values())
            assertTrue(journal.containsSignature(signature));

        journal.markDiscoveryComplete();
        LocalQdnContentJournal.resetForTesting();
        journal = LocalQdnContentJournal.getInstance();
        assertTrue(journal.isDiscoveryComplete());
        assertEquals(10, journal.size());
    }

    @Test
    public void dirtyCandidateSelectionDoesNotStarveLaterSignatures() {
        LocalQdnContentJournal journal = LocalQdnContentJournal.getInstance();
        for (byte value = 10; value < 15; ++value)
            journal.recordFilesystemChange(signature(value));

        Set<Byte> visited = new HashSet<>();
        for (int pass = 0; pass < 3; ++pass) {
            for (byte[] candidate : journal.nextCandidates(2, false))
                visited.add(candidate[0]);
        }

        assertEquals(5, visited.size());
    }

    @Test
    public void auditSnapshotTracksChangesAndReturnsDefensiveCopies() {
        byte[] first = signature((byte) 31);
        byte[] second = signature((byte) 32);
        LocalQdnContentJournal journal = LocalQdnContentJournal.getInstance();

        journal.confirmPresent(first);
        List<byte[]> initialAudit = journal.nextCandidates(10, true);
        assertEquals(1, initialAudit.size());
        assertArrayEquals(first, initialAudit.get(0));

        initialAudit.get(0)[0] = 99;
        assertTrue(journal.containsSignature(first));

        journal.confirmPresent(second);
        List<byte[]> expandedAudit = journal.nextCandidates(10, true);
        assertEquals(2, expandedAudit.size());
        assertTrue(expandedAudit.stream().anyMatch(candidate -> Arrays.equals(candidate, first)));
        assertTrue(expandedAudit.stream().anyMatch(candidate -> Arrays.equals(candidate, second)));

        journal.confirmAbsent(first);
        List<byte[]> reducedAudit = journal.nextCandidates(10, true);
        assertEquals(1, reducedAudit.size());
        assertArrayEquals(second, reducedAudit.get(0));
    }

    @Test
    public void cleanupRemainsPausedUntilAllConcurrentBuildsFinish() {
        ArbitraryDataBuildManager buildManager = ArbitraryDataBuildManager.getInstance();
        buildManager.setBuildInProgress(true);
        buildManager.setBuildInProgress(true);
        buildManager.setBuildInProgress(false);
        assertTrue(buildManager.getBuildInProgress());

        buildManager.setBuildInProgress(false);
        assertFalse(buildManager.getBuildInProgress());
    }

    private byte[] signature(byte value) {
        byte[] signature = new byte[64];
        Arrays.fill(signature, value);
        return signature;
    }
}
