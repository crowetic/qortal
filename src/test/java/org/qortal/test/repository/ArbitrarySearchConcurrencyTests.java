package org.qortal.test.repository;

import org.junit.Test;
import org.qortal.arbitrary.misc.Service;
import org.qortal.data.arbitrary.ArbitraryResourceCache;
import org.qortal.data.arbitrary.ArbitraryResourceData;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.*;

public class ArbitrarySearchConcurrencyTests {
    private ArbitraryResourceCache cache() throws Exception {
        var constructor = ArbitraryResourceCache.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        return constructor.newInstance();
    }

    @Test
    public void readersProceedTogetherAndWriterPublishesAfterReadersFinish() throws Exception {
        ArbitraryResourceCache cache = cache();
        ArbitraryResourceData original = new ArbitraryResourceData(Service.DOCUMENT, "Name", null);
        cache.put(original);
        CountDownLatch reading = new CountDownLatch(1), release = new CountDownLatch(1);
        List<String> blockingNames = new AbstractList<>() {
            public int size() { return 1; }
            public String get(int index) {
                reading.countDown();
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("reader not released");
                } catch (InterruptedException e) { throw new AssertionError(e); }
                return "Name";
            }
        };
        ExecutorService executor = Executors.newFixedThreadPool(3);
        try {
            Future<List<ArbitraryResourceData>> first = executor.submit(() -> query(cache, blockingNames));
            assertTrue(reading.await(5, TimeUnit.SECONDS));
            // Must complete while the first reader still holds the index read lock.
            assertSame(original, executor.submit(() -> query(cache, null)).get(2, TimeUnit.SECONDS).get(0));
            ArbitraryResourceData updated = new ArbitraryResourceData(Service.DOCUMENT, "Name", null);
            Future<?> writer = executor.submit(() -> cache.put(updated));
            release.countDown();
            assertSame(original, first.get(5, TimeUnit.SECONDS).get(0));
            writer.get(5, TimeUnit.SECONDS);
            assertSame(updated, query(cache, null).get(0));
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void candidateSnapshotsKeepMembershipAndRefreshNonIndexedFields() throws Exception {
        ArbitraryResourceCache cache = cache();
        ArbitraryResourceData original = new ArbitraryResourceData(Service.DOCUMENT, "Name", null);
        original.size = 1;
        cache.put(original);
        List<ArbitraryResourceData> snapshot = query(cache, null);
        ArbitraryResourceData updated = new ArbitraryResourceData(Service.DOCUMENT, "Name", null);
        updated.size = 2;
        cache.replaceAll(Map.of(Service.DOCUMENT.value, new LinkedHashMap<>(Map.of(ArbitraryResourceCache.resourceKey("Name", null), updated))));
        assertSame(original, snapshot.get(0));
        assertSame(updated, query(cache, null).get(0));
        cache.put(new ArbitraryResourceData(Service.DOCUMENT, "Other", null));
        assertEquals(1, snapshot.size());
        assertEquals(2, query(cache, null).size());
        cache.replaceAll(Map.of());
        assertTrue(query(cache, null).isEmpty());
        assertEquals(1, snapshot.size());
    }

    @Test
    public void repeatedRefreshesNeverExposeMixedGenerationsToConcurrentSearches() throws Exception {
        ArbitraryResourceCache cache = cache();
        java.util.function.IntFunction<Map<Integer, Map<String, ArbitraryResourceData>>> generation = number -> {
            Map<String, ArbitraryResourceData> rows = new LinkedHashMap<>();
            for (int i = 0; i < 80; ++i) {
                ArbitraryResourceData row = new ArbitraryResourceData(Service.DOCUMENT, "Name" + i, null);
                row.created = (long) number;
                rows.put(ArbitraryResourceCache.resourceKey(row.name, row.identifier), row);
            }
            return Map.of(Service.DOCUMENT.value, rows);
        };
        cache.replaceAll(generation.apply(0));
        ExecutorService executor = Executors.newFixedThreadPool(9);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> tasks = new ArrayList<>();
        try {
            for (int reader = 0; reader < 8; ++reader) {
                tasks.add(executor.submit(() -> {
                    start.await();
                    for (int i = 0; i < 300; ++i) {
                        List<ArbitraryResourceData> rows = query(cache, i % 2 == 0 ? null : List.of("Name1", "Name2"));
                        assertEquals(i % 2 == 0 ? 80 : 2, rows.size());
                        Long version = rows.get(0).created;
                        for (ArbitraryResourceData row : rows) assertEquals(version, row.created);
                    }
                    return null;
                }));
            }
            tasks.add(executor.submit(() -> {
                start.await();
                for (int i = 1; i <= 200; ++i) cache.replaceAll(generation.apply(i));
                return null;
            }));
            start.countDown();
            for (Future<?> task : tasks) task.get(15, TimeUnit.SECONDS);
            assertEquals(Long.valueOf(200), query(cache, null).get(0).created);
        } finally {
            start.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private List<ArbitraryResourceData> query(ArbitraryResourceCache cache, List<String> exactNames) {
        return cache.indexedCandidates(Service.DOCUMENT.value, null, null, null, null, null,
                false, exactNames, null, false);
    }
}
