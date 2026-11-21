package org.qortal.test.repository;

import org.junit.Test;
import org.qortal.arbitrary.misc.Service;
import org.qortal.data.arbitrary.ArbitraryResourceCache;
import org.qortal.data.arbitrary.ArbitraryResourceData;
import org.qortal.data.arbitrary.ArbitraryResourceMetadata;
import org.qortal.data.arbitrary.ArbitraryResourceStatus.Status;

import java.util.ArrayList;
import java.util.Set;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class ArbitraryResourceCacheRefreshTests {
    private static ArbitraryResourceCache newCache() throws Exception {
        var constructor = ArbitraryResourceCache.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        return constructor.newInstance();
    }

    @Test
    public void unchangedIndexedFieldsExposeAllFreshDataWithoutMutatingEarlierResults() throws Exception {
        ArbitraryResourceCache cache = newCache();
        ArbitraryResourceData old = resource(1, "Title", "Description");
        old.setStatus(Status.PUBLISHED);
        cache.replaceAll(snapshot(old));
        List<ArbitraryResourceData> earlierResults = query(cache, "title", true);

        ArbitraryResourceData updated = resource(1, "Title", "Description");
        updated.setStatus(Status.READY);
        updated.size = 123;
        updated.created = 2L;
        updated.updated = 3L;
        updated.latestSignature = new byte[]{1, 2, 3};
        updated.metadata.setTags(List.of("new tag"));
        cache.replaceAll(snapshot(updated));

        // Both tree-backed and linear searches must reference the complete new row,
        // even when none of the four indexed fields changed.
        for (String term : List.of("name", "identifier", "title", "description")) {
            assertSame(updated, query(cache, term, true).get(0));
            assertSame(updated, query(cache, term, false).get(0));
        }
        assertSame(updated, all(cache).get(0));
        assertSame(updated, cache.getDataByService().get(Service.WEBSITE.value)
                .get(ArbitraryResourceCache.resourceKey(updated.name, updated.identifier)));
        assertSame(old, earlierResults.get(0));
        assertEquals(Status.PUBLISHED, old.status.getStatus());
    }

    @Test
    public void refreshRemovesDeletedRowsServicesAndOldMetadataKeys() throws Exception {
        ArbitraryResourceCache cache = newCache();
        ArbitraryResourceData old = resource(1, "Old title", "Old description");
        ArbitraryResourceData deleted = resource(2, "Deleted title", "Deleted description");
        ArbitraryResourceData removedService = resource(3, "Service title", "Service description");
        removedService.service = Service.DOCUMENT;
        cache.replaceAll(snapshot(old, deleted, removedService));

        ArbitraryResourceData changed = resource(1, "New title", null);
        ArbitraryResourceData added = resource(4, "Added title", "Added description");
        cache.replaceAll(snapshot(changed, added));
        for (String term : List.of("old", "deleted", "service")) {
            assertTrue(query(cache, term, true).isEmpty());
            assertTrue(query(cache, term, false).isEmpty());
        }
        assertSame(changed, query(cache, "new", true).get(0));
        assertFalse(cache.getDataByService().containsKey(Service.DOCUMENT.value));
        assertTrue(cache.indexedCandidates(Service.DOCUMENT.value, null, null, null, null,
                null, false, null, null, false).isEmpty());
        assertEquals(List.of(changed, added), all(cache));

        changed = resource(1, null, null);
        changed.identifier = null;
        cache.replaceAll(snapshot(changed));
        assertTrue(query(cache, "new", true).isEmpty());
        assertTrue(query(cache, "identifier", true).isEmpty());
        assertSame(changed, all(cache).get(0));
    }

    @Test
    public void smallRefreshAppliesChangedAddedAndDeletedRowsTogether() throws Exception {
        ArbitraryResourceCache cache = newCache();
        List<ArbitraryResourceData> rows = new ArrayList<>();
        for (int i = 0; i < 8; i++) rows.add(resource(i, "Title " + i, "Description " + i));
        cache.replaceAll(snapshot(rows.toArray(new ArbitraryResourceData[0])));
        rows.set(0, resource(0, "Changed title", "Changed description"));
        rows.set(1, resource(9, "Added title", "Added description"));
        cache.replaceAll(snapshot(rows.toArray(new ArbitraryResourceData[0])));
        assertTrue(query(cache, "title 0", true).isEmpty());
        assertTrue(query(cache, "name1", true).isEmpty());
        assertSame(rows.get(0), query(cache, "changed", true).get(0));
        assertSame(rows.get(1), query(cache, "added", true).get(0));
        assertSameResults(rows, all(cache));
    }

    @Test
    public void mutableMetadataAndDefaultIdentifierTransitionsDoNotLeaveStaleKeys() throws Exception {
        ArbitraryResourceCache cache = newCache();
        ArbitraryResourceData data = resource(1, "Old title", "Old description");
        data.identifier = "default";
        cache.replaceAll(snapshot(data));
        data.metadata.setTitle("New title");
        cache.replaceAll(snapshot(data));
        assertTrue(query(cache, "old title", true).isEmpty());
        assertSame(data, query(cache, "new title", true).get(0));
        data.identifier = null;
        cache.replaceAll(snapshot(data));
        assertTrue(query(cache, "default", true).isEmpty());
        assertSame(data, all(cache).get(0));
        data.identifier = "default";
        cache.replaceAll(snapshot(data));
        assertSame(data, query(cache, "default", true).get(0));
    }

    @Test
    public void refreshPreservesIncomingOrderEvenWhenReusingIndexes() throws Exception {
        ArbitraryResourceCache cache = newCache();
        ArbitraryResourceData a = resource(1, "Title", "Description");
        ArbitraryResourceData b = resource(2, "Title", "Description");
        cache.replaceAll(snapshot(a, b));
        cache.replaceAll(snapshot(b, a));
        assertEquals(List.of(b, a), all(cache));
        assertEquals(List.of(b, a), query(cache, "description", false));
    }

    @Test
    public void commitsDuringDatabaseReadSurvivePublicationIncludingNewServices() throws Exception {
        ArbitraryResourceCache cache = newCache();
        ArbitraryResourceData old = resource(1, "Old title", null);
        ArbitraryResourceData deleted = resource(2, "Deleted", null);
        cache.replaceAll(snapshot(old, deleted));
        ArbitraryResourceData latest = resource(1, "Latest title", null);
        ArbitraryResourceData newService = resource(3, "New service", null);
        newService.service = Service.DOCUMENT;
        try (ArbitraryResourceCache.Refresh refresh = cache.beginRefresh()) {
            cache.put(resource(1, "Intermediate title", null));
            cache.put(latest);
            cache.put(newService);
            refresh.replaceAll(snapshot(old));
        }
        assertSame(latest, all(cache).get(0));
        assertEquals(1, all(cache).size());
        assertTrue(query(cache, "old", true).isEmpty());
        assertSame(newService, cache.indexedCandidates(Service.DOCUMENT.value, null, null, null,
                null, null, false, null, null, false).get(0));
        // A completed refresh must not retain/replay its updates forever.
        try (ArbitraryResourceCache.Refresh refresh = cache.beginRefresh()) {
            refresh.replaceAll(snapshot(old));
        }
        assertSame(old, all(cache).get(0));
        assertFalse(cache.getDataByService().containsKey(Service.DOCUMENT.value));
    }

    @Test
    public void abandonedRefreshDoesNotPublishOrReplayItsUpdatesLater() throws Exception {
        ArbitraryResourceCache cache = newCache();
        ArbitraryResourceData old = resource(1, "Old", null);
        ArbitraryResourceData updated = resource(1, "Updated", null);
        cache.replaceAll(snapshot(old));
        ArbitraryResourceCache.Refresh abandoned = cache.beginRefresh();
        cache.put(updated);
        abandoned.close();
        assertSame(updated, all(cache).get(0));
        try {
            abandoned.replaceAll(snapshot(old));
            fail("A closed refresh must not publish");
        } catch (IllegalStateException expected) {
            // Expected.
        }
        try (ArbitraryResourceCache.Refresh refresh = cache.beginRefresh()) {
            refresh.replaceAll(snapshot(old));
        }
        assertSame(old, all(cache).get(0));
    }

    @Test
    public void searchesWaitForCompleteServiceInsteadOfObservingAnEmptyIndex() throws Exception {
        ArbitraryResourceCache cache = newCache();
        cache.replaceAll(snapshot(resource(1, "Old", null)));
        ArbitraryResourceData updated = resource(1, "New", null);
        CountDownLatch insideRefresh = new CountDownLatch(1);
        CountDownLatch releaseRefresh = new CountDownLatch(1);
        Map<String, ArbitraryResourceData> blocking = new LinkedHashMap<>() {
            @Override
            public Set<Map.Entry<String, ArbitraryResourceData>> entrySet() {
                insideRefresh.countDown();
                try {
                    if (!releaseRefresh.await(5, TimeUnit.SECONDS))
                        throw new AssertionError("Refresh was not released");
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                return super.entrySet();
            }
        };
        blocking.put(ArbitraryResourceCache.resourceKey(updated.name, updated.identifier), updated);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var writer = executor.submit(() -> cache.replaceAll(Map.of(Service.WEBSITE.value, blocking)));
            assertTrue(insideRefresh.await(5, TimeUnit.SECONDS));
            CountDownLatch readerStarted = new CountDownLatch(1);
            var reader = executor.submit(() -> { readerStarted.countDown(); return all(cache); });
            assertTrue(readerStarted.await(5, TimeUnit.SECONDS));
            releaseRefresh.countDown();
            writer.get(5, TimeUnit.SECONDS);
            assertSame(updated, reader.get(5, TimeUnit.SECONDS).get(0));
        } finally {
            releaseRefresh.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void repeatedReconciliationMatchesFreshRebuildAcrossQueryModes() throws Exception {
        ArbitraryResourceCache reused = newCache();
        Random random = new Random(78123);
        for (int round = 0; round < 30; round++) {
            List<ArbitraryResourceData> rows = new ArrayList<>();
            for (int i = 0; i < 120; i++) {
                if (random.nextInt(5) == 0) continue;
                ArbitraryResourceData data = resource(i, random.nextBoolean() ? "Alpha " + i : null,
                        random.nextBoolean() ? "Beta keyword " + i : null);
                if (random.nextBoolean()) data.identifier = null;
                rows.add(data);
            }
            Map<Integer, Map<String, ArbitraryResourceData>> snapshot = snapshot(rows.toArray(new ArbitraryResourceData[0]));
            reused.replaceAll(snapshot);
            ArbitraryResourceCache rebuilt = newCache();
            rebuilt.replaceAll(snapshot);
            for (boolean prefix : List.of(true, false))
                for (String term : new String[]{null, "name1", "alpha", "beta", "identifier", "absent"})
                    assertSameResults(query(rebuilt, term, prefix), query(reused, term, prefix));
            for (boolean prefix : List.of(true, false)) {
                assertSameResults(rebuilt.indexedCandidates(Service.WEBSITE.value, null, "identifier1",
                                List.of("Name1"), "alpha", "beta", prefix, null, List.of("keyword"), false),
                        reused.indexedCandidates(Service.WEBSITE.value, null, "identifier1",
                                List.of("Name1"), "alpha", "beta", prefix, null, List.of("keyword"), false));
                assertSameResults(rebuilt.indexedCandidates(Service.WEBSITE.value, "name", null,
                                null, null, null, prefix, List.of("NAME1", "NAME2"), null, true),
                        reused.indexedCandidates(Service.WEBSITE.value, "name", null,
                                null, null, null, prefix, List.of("NAME1", "NAME2"), null, true));
            }
        }
    }

    private static void assertSameResults(List<ArbitraryResourceData> expected, List<ArbitraryResourceData> actual) {
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) assertSame(expected.get(i), actual.get(i));
    }

    private static List<ArbitraryResourceData> all(ArbitraryResourceCache cache) {
        return query(cache, null, false);
    }

    private static List<ArbitraryResourceData> query(ArbitraryResourceCache cache, String query, boolean prefix) {
        return cache.indexedCandidates(Service.WEBSITE.value, query, null, null, null, null,
                prefix, null, null, false);
    }

    private static Map<Integer, Map<String, ArbitraryResourceData>> snapshot(ArbitraryResourceData... rows) {
        Map<Integer, Map<String, ArbitraryResourceData>> result = new HashMap<>();
        for (ArbitraryResourceData data : rows)
            result.computeIfAbsent(data.service.value, ignored -> new LinkedHashMap<>())
                    .put(ArbitraryResourceCache.resourceKey(data.name, data.identifier), data);
        return result;
    }

    private static ArbitraryResourceData resource(int id, String title, String description) {
        ArbitraryResourceData data = new ArbitraryResourceData(Service.WEBSITE, "Name" + id, "Identifier" + id);
        data.created = 1L;
        if (title != null || description != null) {
            data.metadata = new ArbitraryResourceMetadata();
            data.metadata.setTitle(title);
            data.metadata.setDescription(description);
        }
        return data;
    }
}
