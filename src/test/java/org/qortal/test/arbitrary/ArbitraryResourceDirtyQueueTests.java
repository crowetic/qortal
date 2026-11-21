package org.qortal.test.arbitrary;

import org.junit.Before;
import org.junit.Test;
import org.qortal.arbitrary.misc.Service;
import org.qortal.data.arbitrary.ArbitraryResourceDirtyData;
import org.qortal.data.transaction.ArbitraryTransactionData;
import org.qortal.data.transaction.BaseTransactionData;
import org.qortal.group.Group;
import org.qortal.repository.Repository;
import org.qortal.repository.RepositoryManager;
import org.qortal.repository.hsqldb.HSQLDBDatabaseUpdates;
import org.qortal.repository.hsqldb.HSQLDBRepositoryFactory;
import org.qortal.settings.Settings;
import org.qortal.test.common.Common;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.*;

public class ArbitraryResourceDirtyQueueTests extends Common {

    @Before
    public void beforeTest() throws Exception {
        Common.useDefaultSettings();
    }

    private ArbitraryTransactionData transaction(String name, String identifier, byte signatureByte) {
        byte[] signature = new byte[64];
        signature[0] = signatureByte;
        BaseTransactionData base = new BaseTransactionData(System.currentTimeMillis(), Group.NO_GROUP,
                new byte[64], new byte[32], 0L, signature);
        return new ArbitraryTransactionData(base, 5, Service.WEBSITE.value, 0, 1,
                name, identifier, ArbitraryTransactionData.Method.PUT, null,
                ArbitraryTransactionData.Compression.NONE, new byte[] { 1 },
                ArbitraryTransactionData.DataType.RAW_DATA, null, Collections.emptyList());
    }

    @Test
    public void freshGenesisDatabaseUsesExpandedCacheFileScale() throws Exception {
        try (Repository repository = RepositoryManager.getRepository();
             Statement statement = repository.getConnection().createStatement();
             ResultSet resultSet = statement.executeQuery(
                     "SELECT PROPERTY_VALUE FROM INFORMATION_SCHEMA.SYSTEM_PROPERTIES " +
                             "WHERE PROPERTY_NAME = 'hsqldb.cache_file_scale'")) {
            assertTrue(resultSet.next());
            assertEquals("128", resultSet.getString(1));
        }
    }

    @Test
    public void freshBootstrapDatabaseKeepsDefaultCacheFileScale() throws Exception {
        String bootstrapSettings = getClass().getClassLoader().getResource("test-settings-v2-bootstrap.json").getPath();
        String defaultSettings = getClass().getClassLoader().getResource(Common.testSettingsFilename).getPath();
        Settings.fileInstance(bootstrapSettings);
        try {
            HSQLDBRepositoryFactory factory = new HSQLDBRepositoryFactory(
                    "jdbc:hsqldb:mem:bootstrap-scale-" + UUID.randomUUID());
            try (Repository repository = factory.getRepository();
                 Statement statement = repository.getConnection().createStatement();
                 ResultSet resultSet = statement.executeQuery(
                         "SELECT PROPERTY_VALUE FROM INFORMATION_SCHEMA.SYSTEM_PROPERTIES " +
                                 "WHERE PROPERTY_NAME = 'hsqldb.cache_file_scale'")) {
                assertTrue(resultSet.next());
                assertEquals("32", resultSet.getString(1));
            } finally {
                factory.close();
            }
        } finally {
            Settings.fileInstance(defaultSettings);
        }
    }

    @Test
    public void dirtyEventsCoalesceAndRevisionProtectsNewerWork() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            ArbitraryTransactionData first = transaction("MixedCaseName", null, (byte) 1);
            repository.getArbitraryRepository().markResourceDirty(first, ArbitraryResourceDirtyData.HEAD_DIRTY);
            repository.saveChanges();

            List<ArbitraryResourceDirtyData> firstRead = repository.getArbitraryRepository()
                    .getDirtyResources(10, System.currentTimeMillis());
            assertEquals(1, firstRead.size());
            assertEquals(1L, firstRead.get(0).getRevision());
            repository.discardChanges();

            ArbitraryTransactionData second = transaction("mixedcasename", null, (byte) 2);
            repository.getArbitraryRepository().markResourceDirty(second, ArbitraryResourceDirtyData.STATUS_DIRTY);
            repository.saveChanges();

            List<ArbitraryResourceDirtyData> coalesced = repository.getArbitraryRepository()
                    .getDirtyResources(10, System.currentTimeMillis());
            assertEquals(1, coalesced.size());
            assertEquals(2L, coalesced.get(0).getRevision());
            assertTrue(coalesced.get(0).hasFlag(ArbitraryResourceDirtyData.HEAD_DIRTY));
            assertTrue(coalesced.get(0).hasFlag(ArbitraryResourceDirtyData.STATUS_DIRTY));
            assertArrayEquals(second.getSignature(), coalesced.get(0).getTriggeringSignature());

            repository.getArbitraryRepository().clearDirtyResource(firstRead.get(0));
            repository.saveChanges();
            assertEquals(1, repository.getArbitraryRepository()
                    .getDirtyResources(10, System.currentTimeMillis()).size());
            repository.discardChanges();

            repository.getArbitraryRepository().clearDirtyResource(coalesced.get(0));
            repository.saveChanges();
            assertTrue(repository.getArbitraryRepository()
                    .getDirtyResources(10, System.currentTimeMillis()).isEmpty());
            repository.discardChanges();
        }
    }

    @Test
    public void rollbackAndRetrySchedulingAreDurable() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            ArbitraryTransactionData data = transaction("retry-name", "id", (byte) 3);
            repository.getArbitraryRepository().markResourceDirty(data, ArbitraryResourceDirtyData.METADATA_DIRTY);
            repository.discardChanges();
            assertTrue(repository.getArbitraryRepository()
                    .getDirtyResources(10, System.currentTimeMillis()).isEmpty());
            repository.discardChanges();

            repository.getArbitraryRepository().markResourceDirty(data, ArbitraryResourceDirtyData.METADATA_DIRTY);
            repository.saveChanges();
            ArbitraryResourceDirtyData dirty = repository.getArbitraryRepository()
                    .getDirtyResources(10, System.currentTimeMillis()).get(0);
            repository.discardChanges();

            long nextAttempt = System.currentTimeMillis() + 60_000L;
            repository.getArbitraryRepository().deferDirtyResource(dirty, nextAttempt);
            repository.saveChanges();
            assertTrue(repository.getArbitraryRepository()
                    .getDirtyResources(10, nextAttempt - 1).isEmpty());
            List<ArbitraryResourceDirtyData> retried = repository.getArbitraryRepository()
                    .getDirtyResources(10, nextAttempt);
            assertEquals(1, retried.size());
            assertEquals(1, retried.get(0).getRetryCount());
            repository.discardChanges();
        }
    }

    @Test
    public void bootstrapSanitizationKeepsOnlyChainHeadWork() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            ArbitraryTransactionData local = transaction("local-only", null, (byte) 4);
            repository.getArbitraryRepository().markResourceDirty(local,
                    ArbitraryResourceDirtyData.METADATA_DIRTY | ArbitraryResourceDirtyData.STATUS_DIRTY);

            ArbitraryTransactionData chain = transaction("chain-work", null, (byte) 5);
            repository.getArbitraryRepository().markResourceDirty(chain,
                    ArbitraryResourceDirtyData.HEAD_DIRTY | ArbitraryResourceDirtyData.STATUS_DIRTY);
            repository.saveChanges();

            ArbitraryResourceDirtyData chainBefore = repository.getArbitraryRepository()
                    .getDirtyResources(10, System.currentTimeMillis()).stream()
                    .filter(row -> row.getName().equals("chain-work"))
                    .findFirst().orElseThrow(AssertionError::new);
            repository.getArbitraryRepository().deferDirtyResource(chainBefore,
                    System.currentTimeMillis() + 60_000L);
            repository.saveChanges();

            repository.getArbitraryRepository().sanitizeDirtyResourcesForBootstrap();
            repository.saveChanges();

            List<ArbitraryResourceDirtyData> rows = repository.getArbitraryRepository()
                    .getDirtyResources(10, Long.MAX_VALUE);
            assertEquals(1, rows.size());
            ArbitraryResourceDirtyData sanitized = rows.get(0);
            assertEquals("chain-work", sanitized.getName());
            assertEquals(ArbitraryResourceDirtyData.HEAD_DIRTY, sanitized.getFlags());
            assertNull(sanitized.getTriggeringSignature());
            assertEquals(0, sanitized.getRetryCount());
        }
    }

    @Test
    public void schemaMigrationCanResumeAfterPartialCompletion() throws Exception {
        try (Repository repository = RepositoryManager.getRepository();
             Statement statement = repository.getConnection().createStatement()) {
            // Simulate a crash after every schema object was created but before the
            // DatabaseInfo version increment committed.
            statement.executeUpdate("UPDATE DatabaseInfo SET version = 56");
            repository.getConnection().commit();

            HSQLDBDatabaseUpdates.updateDatabase(repository.getConnection());
            assertEquals(57, HSQLDBDatabaseUpdates.fetchDatabaseVersion(repository.getConnection()));

            try (ResultSet resultSet = statement.executeQuery(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'ARBITRARYRESOURCEDIRTY'")) {
                assertTrue(resultSet.next());
                assertEquals(1, resultSet.getInt(1));
            }

            try (ResultSet resultSet = statement.executeQuery(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.SYSTEM_INDEXINFO " +
                            "WHERE INDEX_NAME = 'ARBITRARYCREATEDINDEX'")) {
                assertTrue(resultSet.next());
                assertTrue(resultSet.getInt(1) > 0);
            }
        }
    }

    @Test
    public void compatibilityMigrationPreservesQueueAndStatus() throws Exception {
        try (Repository repository = RepositoryManager.getRepository();
             Statement statement = repository.getConnection().createStatement()) {
            ArbitraryTransactionData statusOnly = transaction("status-only", null, (byte) 7);
            repository.getArbitraryRepository().markResourceDirty(statusOnly, ArbitraryResourceDirtyData.STATUS_DIRTY);
            ArbitraryTransactionData mixed = transaction("mixed-work", null, (byte) 8);
            repository.getArbitraryRepository().markResourceDirty(mixed,
                    ArbitraryResourceDirtyData.HEAD_DIRTY | ArbitraryResourceDirtyData.STATUS_DIRTY);
            repository.saveChanges();

            statement.executeUpdate("INSERT INTO ArbitraryResourcesCache " +
                    "(service, name, lower_case_name, identifier, size, status, created_when) VALUES " +
                    "(" + Service.WEBSITE.value + ", 'local-status', 'local-status', 'default', 1, 6, 1)");

            statement.executeUpdate("UPDATE DatabaseInfo SET version = 56");
            repository.getConnection().commit();
            HSQLDBDatabaseUpdates.updateDatabase(repository.getConnection());

            List<ArbitraryResourceDirtyData> rows = repository.getArbitraryRepository()
                    .getDirtyResources(10, System.currentTimeMillis());
            assertEquals(2, rows.size());
            ArbitraryResourceDirtyData statusRow = rows.stream()
                    .filter(row -> row.getName().equals("status-only"))
                    .findFirst().orElseThrow(AssertionError::new);
            assertTrue(statusRow.hasFlag(ArbitraryResourceDirtyData.STATUS_DIRTY));
            ArbitraryResourceDirtyData mixedRow = rows.stream()
                    .filter(row -> row.getName().equals("mixed-work"))
                    .findFirst().orElseThrow(AssertionError::new);
            assertTrue(mixedRow.hasFlag(ArbitraryResourceDirtyData.HEAD_DIRTY));
            assertTrue(mixedRow.hasFlag(ArbitraryResourceDirtyData.STATUS_DIRTY));
            try (ResultSet resultSet = statement.executeQuery(
                    "SELECT status FROM ArbitraryResourcesCache WHERE name = 'local-status'")) {
                assertTrue(resultSet.next());
                assertEquals(6, resultSet.getInt(1));
            }
        }
    }
}
