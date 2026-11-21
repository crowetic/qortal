package org.qortal.test.repository;

import org.junit.Test;
import org.qortal.api.SearchMode;
import org.qortal.arbitrary.misc.Service;
import org.qortal.data.arbitrary.ArbitraryResourceData;
import org.qortal.data.arbitrary.ArbitraryResourceMetadata;
import org.qortal.repository.hsqldb.HSQLDBCacheUtils;

import java.util.*;

import static org.junit.Assert.*;

public class ArbitrarySearchPaginationTests {
    @Test
    public void boundedPagesMatchFullStableSortAcrossFiltersAndModes() {
        Random random = new Random(783113);
        List<ArbitraryResourceData> rows = new ArrayList<>();
        for (int i = 0; i < 600; i++) {
            ArbitraryResourceData row = new ArbitraryResourceData(i % 3 == 0 ? Service.WEBSITE : Service.DOCUMENT,
                    "Name" + random.nextInt(40), i % 4 == 0 ? null : "id" + i);
            row.created = i % 17 == 0 ? null : (long) random.nextInt(12);
            row.metadata = new ArbitraryResourceMetadata();
            row.metadata.setTitle("Title " + i % 5);
            row.metadata.setDescription("Keyword " + i % 7);
            rows.add(row);
        }
        for (int trial = 0; trial < 500; trial++) {
            Collections.shuffle(rows, random);
            Map<String, Object> options = new HashMap<>();
            options.put("mode", random.nextBoolean() ? SearchMode.ALL : SearchMode.LATEST);
            options.put("reverse", random.nextBoolean());
            if (random.nextBoolean()) options.put("service", Service.DOCUMENT);
            if (random.nextInt(3) == 0) options.put("exact", List.of("NAME1", "name3", "name21"));
            if (random.nextInt(3) == 0) options.put("query", "Name1");
            if (random.nextInt(3) == 0) options.put("keywords", List.of("word 2", "word 5"));
            if (random.nextInt(3) == 0) options.put("default", true);
            List<ArbitraryResourceData> sorted = filter(rows, options, null, null);
            int offset = random.nextInt(35);
            int limit = 1 + random.nextInt(30);
            List<ArbitraryResourceData> expected = sorted.subList(Math.min(offset, sorted.size()),
                    Math.min(offset + limit, sorted.size()));
            // Metadata/status are retained: comparing references also detects equal-date tie changes.
            List<ArbitraryResourceData> actual = filter(rows, options, limit, offset);
            assertEquals(expected.size(), actual.size());
            for (int i = 0; i < expected.size(); ++i)
                assertSame("trial " + trial + " position " + i, expected.get(i), actual.get(i));
        }
    }

    @Test
    public void paginationRetainsUnlimitedAndLargeOffsetSemantics() {
        List<ArbitraryResourceData> rows = new ArrayList<>();
        for (int i = 0; i < 5000; ++i) {
            ArbitraryResourceData row = new ArbitraryResourceData(Service.DOCUMENT, "Name" + i, null);
            row.created = (long) (i % 9);
            rows.add(row);
        }
        for (boolean reverse : List.of(false, true)) {
            Map<String, Object> options = Map.of("mode", SearchMode.ALL, "reverse", reverse);
            List<ArbitraryResourceData> sorted = filter(rows, options, null, null);
            assertEquals(sorted.subList(20, sorted.size()), filter(rows, options, 0, 20));
            assertEquals(sorted.subList(20, sorted.size()), filter(rows, options, -1, 20));
            for (int offset : new int[]{0, 20, 4095, 4096, 4999, 5000, Integer.MAX_VALUE}) {
                for (int limit : new int[]{1, 20, 4096, Integer.MAX_VALUE}) {
                    int end = (int) Math.min((long) offset + limit, sorted.size());
                    assertEquals(sorted.subList(Math.min(offset, sorted.size()), end),
                            filter(rows, options, limit, offset));
                }
            }
        }
    }

    @Test
    public void boundedPagesPreserveAccessFiltersDatesAndReturnedFields() {
        List<ArbitraryResourceData> rows = new ArrayList<>();
        for (int i = 0; i < 600; ++i) {
            ArbitraryResourceData row = new ArbitraryResourceData(Service.DOCUMENT, "Name" + i % 8, "id" + i);
            row.created = (long) (i % 50);
            row.updated = (long) i + 1000;
            row.size = i;
            row.latestSignature = new byte[]{(byte) i, (byte) (i >> 8)};
            row.metadata = new ArbitraryResourceMetadata();
            row.metadata.setTitle("Title " + i);
            row.setStatus(org.qortal.data.arbitrary.ArbitraryResourceStatus.Status.READY);
            rows.add(row);
        }
        Collections.shuffle(rows, new Random(37731));
        for (SearchMode mode : SearchMode.values())
            for (boolean reverse : List.of(false, true))
                for (boolean metadata : List.of(false, true))
                    for (boolean status : List.of(false, true)) {
                        Map<String, Object> options = new HashMap<>();
                        options.put("mode", mode);
                        options.put("reverse", reverse);
                        options.put("metadata", metadata);
                        options.put("status", status);
                        options.put("after", 5L);
                        options.put("before", 40L);
                        options.put("minLevel", 2);
                        options.put("include", (java.util.function.Supplier<List<String>>) () -> List.of("Name1", "Name2", "Name3"));
                        options.put("exclude", (java.util.function.Supplier<List<String>>) () -> List.of("Name1"));
                        List<ArbitraryResourceData> all = filter(rows, options, null, null);
                        assertFalse(all.isEmpty());
                        List<ArbitraryResourceData> page = filter(rows, options, 5, 1);
                        List<ArbitraryResourceData> expected = all.subList(1, Math.min(6, all.size()));
                        assertEquals(expected.size(), page.size());
                        for (int i = 0; i < page.size(); ++i) {
                            ArbitraryResourceData a = expected.get(i), b = page.get(i);
                            assertEquals(a.name, b.name);
                            assertEquals(a.service, b.service);
                            assertEquals(a.identifier, b.identifier);
                            assertEquals(a.created, b.created);
                            assertEquals(a.updated, b.updated);
                            assertEquals(a.size, b.size);
                            assertArrayEquals(a.latestSignature, b.latestSignature);
                            assertSame(a.metadata, b.metadata);
                            assertSame(a.status, b.status);
                        }
                    }
        for (ArbitraryResourceData row : rows) {
            assertNotNull(row.metadata);
            assertNotNull(row.status);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> Optional<T> option(Map<String, Object> options, String key) {
        return Optional.ofNullable((T) options.get(key));
    }

    private static List<ArbitraryResourceData> filter(List<ArbitraryResourceData> rows,
            Map<String, Object> options, Integer limit, Integer offset) {
        return HSQLDBCacheUtils.filterList(rows, Map.of("Name1", 1, "Name2", 2, "Name3", 3), option(options, "mode"), option(options, "service"),
                option(options, "query"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                false, option(options, "exact"), option(options, "keywords"), options.containsKey("default"),
                option(options, "minLevel"), option(options, "include"), option(options, "exclude"),
                Optional.of((Boolean) options.getOrDefault("metadata", true)),
                Optional.of((Boolean) options.getOrDefault("status", true)),
                option(options, "before"), option(options, "after"), Optional.ofNullable(limit), Optional.ofNullable(offset),
                option(options, "reverse"));
    }
}
