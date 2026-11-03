package org.qortal.test.repository;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.qortal.api.SearchMode;
import org.qortal.arbitrary.misc.Category;
import org.qortal.arbitrary.misc.Service;
import org.qortal.data.arbitrary.ArbitraryResourceData;
import org.qortal.data.arbitrary.ArbitraryResourceMetadata;
import org.qortal.data.arbitrary.ArbitraryResourceStatus.Status;
import org.qortal.repository.Repository;
import org.qortal.list.ResourceListManager;
import org.qortal.data.naming.NameData;
import org.qortal.data.account.AccountData;
import org.qortal.repository.RepositoryManager;
import org.qortal.settings.Settings;
import org.qortal.test.common.Common;

import java.lang.reflect.Field;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class ArbitraryResourceSearchTests extends Common {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    private Field listsInstance;
    private Object oldListsInstance;
    private Field listsPath;
    private Object oldListsPath;
    private Field cacheEnabled;
    private boolean originalCacheEnabled;

    @Before
    public void beforeTest() throws Exception {
        Common.useDefaultSettings();
        // Exercise the SQL fallback rather than the separate in-memory search implementation.
        cacheEnabled = Settings.class.getDeclaredField("dbCacheEnabled");
        cacheEnabled.setAccessible(true);
        originalCacheEnabled = cacheEnabled.getBoolean(Settings.getInstance());
        cacheEnabled.setBoolean(Settings.getInstance(), false);
        listsInstance = ResourceListManager.class.getDeclaredField("instance");
        listsInstance.setAccessible(true);
        oldListsInstance = listsInstance.get(null);
        listsPath = Settings.class.getDeclaredField("listsPath");
        listsPath.setAccessible(true);
        oldListsPath = listsPath.get(Settings.getInstance());
        listsPath.set(Settings.getInstance(), temporaryFolder.newFolder("lists").toString());
        ResourceListManager.reset();
    }

    @After
    public void restoreCacheSetting() throws Exception {
        cacheEnabled.setBoolean(Settings.getInstance(), originalCacheEnabled);
        listsPath.set(Settings.getInstance(), oldListsPath);
        listsInstance.set(null, oldListsInstance);
    }

    @Test
    public void testDefaultSearchDoesNotReadMetadataTable() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            saveResource(repository, "Alpha", "default", 100L, "Metadata title");
            saveResource(repository, "Beta", "default", 200L, null);
            // A search that does not use metadata must still work when that table is unavailable.
            dropMetadataTable(repository);
            for (Boolean includeMetadata : Arrays.asList(null, false)) {
                List<ArbitraryResourceData> results = search(repository, "alp", null, null, null,
                        true, includeMetadata, SearchMode.ALL, null, null, null);
                assertEquals(1, results.size());
                assertEquals("Alpha", results.get(0).name);
                assertNull(results.get(0).identifier);
                assertNull(results.get(0).metadata);
                assertEquals(Status.PUBLISHED, results.get(0).status.getStatus());
                assertArrayEquals(new byte[64], results.get(0).latestSignature);
            }
        }
    }

    @Test
    public void testNonDefaultSearchWithoutQueryDoesNotReadMetadataTable() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            saveResource(repository, "Alpha", "one", 100L, null);
            saveResource(repository, "Beta", "two", 200L, null);
            dropMetadataTable(repository);
            List<ArbitraryResourceData> results = search(repository, null, null, null, Collections.emptyList(),
                    false, false, SearchMode.ALL, 1, 1, true);
            assertEquals(1, results.size());
            assertEquals("Alpha", results.get(0).name);
            assertEquals("one", results.get(0).identifier);
            assertEquals(Long.valueOf(100L), results.get(0).created);
        }
    }

    @Test
    public void testMetadataFiltersWorkWithoutReturningMetadata() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            saveResource(repository, "Alpha", "default", 100L, "Unique title");
            saveResource(repository, "Beta", "default", 200L, "Other");
            for (Boolean includeMetadata : Arrays.asList(null, false)) {
                // General non-default searches include title and description, even without metadata output.
                assertAlpha(search(repository, "unique", null, null, null,
                        false, includeMetadata, SearchMode.ALL, null, null, null));
                assertAlpha(search(repository, null, "unique", null, null,
                        true, includeMetadata, SearchMode.ALL, null, null, null));
                assertAlpha(search(repository, null, null, "unique", null,
                        true, includeMetadata, SearchMode.ALL, null, null, null));
                assertAlpha(search(repository, null, null, null, Arrays.asList("missing", "unique"),
                        true, includeMetadata, SearchMode.ALL, null, null, null));
                // Default-resource general queries search names only, not metadata.
                assertTrue(search(repository, "unique", null, null, null,
                        true, includeMetadata, SearchMode.ALL, null, null, null).isEmpty());
            }
        }
    }

    @Test
    public void testMetadataOutputIncludesTagsAndKeepsResourcesWithoutMetadata() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            saveResource(repository, "Alpha", "default", 100L, "Unique title");
            saveResource(repository, "Beta", "default", 200L, null);
            List<ArbitraryResourceData> results = search(repository, null, null, null, null,
                    true, true, SearchMode.ALL, null, null, null);
            assertEquals(2, results.size());
            assertEquals("Unique title", results.get(0).metadata.getTitle());
            assertEquals("Unique title description", results.get(0).metadata.getDescription());
            assertEquals(Arrays.asList("tag-one", "tag-two"), results.get(0).metadata.getTags());
            assertEquals(Category.TECHNOLOGY, results.get(0).metadata.getCategory());
            assertArrayEquals(new byte[64], results.get(0).latestSignature);
            assertNull(results.get(1).metadata);
        }
    }

    @Test
    public void testLatestModeAndPaginationWithoutMetadata() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            saveResource(repository, "Alpha", "old", 100L, "Old");
            saveResource(repository, "Alpha", "new", 300L, "New");
            saveResource(repository, "Beta", "default", 200L, null);
            dropMetadataTable(repository);
            for (SearchMode mode : Arrays.asList(null, SearchMode.LATEST)) {
                List<ArbitraryResourceData> results = search(repository, null, null, null, null,
                        false, false, mode, 1, 0, true);
                assertEquals(1, results.size());
                assertEquals("new", results.get(0).identifier);
                results = search(repository, null, null, null, null, false, false, mode, 1, 1, true);
                assertEquals(1, results.size());
                assertEquals("Beta", results.get(0).name);
            }
        }
    }

    @Test
    public void testReportedSearchFiltersBeforePaginationAndKeepsMetadataOptional() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            saveResource(repository, "Alpha", "Post-one", 100, "Matching");
            saveResource(repository, "Beta", "Post-two", 200, "Matching");
            saveResource(repository, "Blocked", "Post-three", 300, "Matching");
            saveResource(repository, "Other", "different", 400, "Matching");
            saveResource(repository, "NoMetadata", "Post-five", 500, null);
            saveResource(repository, Service.FILE, "OtherService", "Post-six", 600, "Matching");
            assertTrue(ResourceListManager.getInstance().addToList("blockedNames", "bLOCKED", false));
            for (boolean includeMetadata : Arrays.asList(false, true)) {
                List<ArbitraryResourceData> results = reportedSearch(repository, includeMetadata, 20, 0, true);
                assertEquals(2, results.size());
                assertEquals("Beta", results.get(0).name);
                assertEquals("Alpha", results.get(1).name);
                assertEquals(includeMetadata, results.get(0).metadata != null);
                assertEquals("Alpha", reportedSearch(repository, includeMetadata, 1, 1, true).get(0).name);
                assertEquals("Alpha", reportedSearch(repository, includeMetadata, 1, 0, false).get(0).name);
                // A zero limit retains the API's unlimited-query behavior.
                assertEquals(2, reportedSearch(repository, includeMetadata, 0, null, null).size());
            }
        }
    }

    private static List<ArbitraryResourceData> reportedSearch(Repository repository, boolean metadata,
            Integer limit, Integer offset, Boolean reverse) throws Exception {
        return repository.getArbitraryRepository().searchArbitraryResources(Service.WEBSITE, null, "pOsT",
                null, null, "mAtCh", null, false, null, false, SearchMode.ALL, null, false, true,
                metadata, true, null, null, limit, offset, reverse);
    }

    @Test
    public void testGeneralQueryKeepsNameAndIdentifierMatchesWithoutMetadata() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            saveResource(repository, "Needle", "one", 100, null);
            saveResource(repository, "Identifier", "needle", 200, null);
            saveResource(repository, "Metadata", "three", 300, "Needle");
            saveResource(repository, "Other", "four", 400, null);
            for (boolean metadata : Arrays.asList(false, true)) {
                List<ArbitraryResourceData> results = search(repository, "needle", null, null, null,
                        false, metadata, SearchMode.ALL, 20, null, true);
                assertEquals(3, results.size());
                assertEquals("Metadata", results.get(0).name);
                assertEquals("Identifier", results.get(1).name);
                assertEquals("Needle", results.get(2).name);
                assertEquals(metadata, results.get(0).metadata != null);
                assertNull(results.get(1).metadata);
                assertNull(results.get(2).metadata);
            }
        }
    }

    @Test
    public void testLatestMetadataFiltersKeepTiesAndDoNotResurrectOlderIdentifiers() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            saveResource(repository, "Alpha", "old", 100, "Match");
            saveResource(repository, "Alpha", "new", 300, "Other");
            saveResource(repository, "Beta", "one", 200, "Match");
            saveResource(repository, "Beta", "two", 200, "Match");
            for (SearchMode mode : Arrays.asList(null, SearchMode.LATEST)) {
                List<ArbitraryResourceData> results = search(repository, null, null, "match", null,
                        false, true, mode, 20, 0, true);
                assertEquals(2, results.size());
                assertEquals("Beta", results.get(0).name);
                assertEquals("Beta", results.get(1).name);
                assertFalse(results.get(0).identifier.equals(results.get(1).identifier));
            }
        }
    }

    @Test
    public void testGlobalSearchPreservesDateBoundsAcrossServices() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            saveResource(repository, "Alpha", "one", 100, "Match");
            saveResource(repository, Service.FILE, "Alpha", "one", 200, "Match");
            saveResource(repository, "Beta", "two", 300, "Match");
            saveResource(repository, "Gamma", "three", 400, "Match");
            for (boolean reverse : Arrays.asList(false, true)) {
                List<ArbitraryResourceData> results = repository.getArbitraryRepository().searchArbitraryResources(
                        null, null, null, null, null, "match", null, false, null, false, SearchMode.ALL,
                        null, false, false, true, false, 400L, 100L, 1, 1, reverse);
                assertEquals(1, results.size());
                assertEquals(Long.valueOf(reverse ? 200 : 300), results.get(0).created);
                assertEquals(reverse ? Service.FILE : Service.WEBSITE, results.get(0).service);
                assertEquals("Match description", results.get(0).metadata.getDescription());
            }
        }
    }

    @Test
    public void testPrefixNameLevelAndFollowedFiltersRemainConjunctive() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            String owner = Common.getTestAccount(repository, "alice").getAddress();
            AccountData account = repository.getAccountRepository().getAccount(owner);
            account.setLevel(5);
            repository.getAccountRepository().setLevel(account);
            repository.getNameRepository().save(new NameData("Alpha", "alpha", owner, "", 1, new byte[64], 0));
            saveResource(repository, "Alpha", "Post-one", 100, "Match");
            saveResource(repository, "Beta", "Post-two", 200, "Match");
            assertTrue(ResourceListManager.getInstance().addToList("followedNames", "aLPHA", false));
            List<ArbitraryResourceData> results = repository.getArbitraryRepository().searchArbitraryResources(
                    Service.WEBSITE, null, "post", Collections.singletonList("al"), "match", null,
                    Arrays.asList("missing", "match"), true, Collections.singletonList("ALPHA"), false,
                    SearchMode.ALL, 5, true, true, true, true, null, null, 20, 0, true);
            assertEquals(1, results.size());
            assertEquals("Alpha", results.get(0).name);
            results = repository.getArbitraryRepository().searchArbitraryResources(
                    Service.WEBSITE, null, "post", null, null, "match", null, true, null, false,
                    SearchMode.ALL, 6, true, false, false, false, null, null, 20, 0, true);
            assertTrue(results.isEmpty());
        }
    }

    private static List<ArbitraryResourceData> search(Repository repository, String query, String title,
            String description, List<String> keywords, boolean defaultResource, Boolean includeMetadata,
            SearchMode mode, Integer limit, Integer offset, Boolean reverse) throws Exception {
        return repository.getArbitraryRepository().searchArbitraryResources(Service.WEBSITE, query, null,
                null, title, description, keywords, false, null, defaultResource, mode, null, false, false,
                includeMetadata, true, null, null, limit, offset, reverse);
    }

    private static void assertAlpha(List<ArbitraryResourceData> results) {
        assertEquals(1, results.size());
        assertEquals("Alpha", results.get(0).name);
        assertNull(results.get(0).metadata);
    }

    private static void dropMetadataTable(Repository repository) throws Exception {
        try (Statement statement = repository.getConnection().createStatement()) {
            statement.execute("DROP TABLE ArbitraryMetadataCache");
        }
    }

    private static void saveResource(Repository repository, String name, String identifier, long created,
            String title) throws Exception {
        saveResource(repository, Service.WEBSITE, name, identifier, created, title);
    }

    private static void saveResource(Repository repository, Service service, String name, String identifier,
            long created, String title) throws Exception {
        ArbitraryResourceData resource = new ArbitraryResourceData();
        resource.name = name;
        resource.service = service;
        resource.identifier = identifier;
        resource.size = 123;
        resource.created = created;
        resource.latestSignature = new byte[64];
        repository.getArbitraryRepository().save(resource);
        if (title != null) {
            ArbitraryResourceMetadata metadata = new ArbitraryResourceMetadata();
            metadata.setArbitraryResourceData(resource);
            metadata.setTitle(title);
            metadata.setDescription(title + " description");
            metadata.setTags(Arrays.asList("tag-one", "tag-two"));
            metadata.setCategory(Category.TECHNOLOGY);
            repository.getArbitraryRepository().save(metadata);
        }
        repository.saveChanges();
    }
}
