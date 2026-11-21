package org.qortal.utils;

import org.junit.Test;
import org.qortal.data.arbitrary.ArbitraryDataIndex;
import org.qortal.data.arbitrary.ArbitraryDataIndexDetail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class ArbitraryIndexGroupingTests {

    @Test
    public void preservesEntriesDuplicatesAndOrderForBothKeys() {
        ArbitraryDataIndexDetail first = detail("alpha", "alice", 3);
        ArbitraryDataIndexDetail second = detail("beta", "alice", 1);
        ArbitraryDataIndexDetail third = detail("alpha", "bob", 2);
        ArbitraryDataIndexDetail singleton = detail("gamma", "carol", 4);
        List<ArbitraryDataIndexDetail> input = List.of(first, second, third, first, singleton);

        Map<String, List<ArbitraryDataIndexDetail>> byTerm =
                ArbitraryIndexUtils.groupIndexDetails(input, entry -> entry.term);
        Map<String, List<ArbitraryDataIndexDetail>> byIssuer =
                ArbitraryIndexUtils.groupIndexDetails(input, entry -> entry.issuer);

        // Details use identity equality: these checks also require the original objects.
        assertEquals(Map.of("alpha", List.of(first, third, first),
                "beta", List.of(second), "gamma", List.of(singleton)), byTerm);
        assertEquals(Map.of("alice", List.of(first, second, first),
                "bob", List.of(third), "carol", List.of(singleton)), byIssuer);
        assertEquals(List.of(first, second, third, first, singleton), input);
        assertEquals(3, first.rank);
        assertEquals(1, second.rank);
        assertEquals(2, third.rank);
    }

    @Test
    public void emptyInputProducesEmptyGroups() {
        assertTrue(ArbitraryIndexUtils.groupIndexDetails(Collections.emptyList(), entry -> entry.term).isEmpty());
        assertTrue(ArbitraryIndexUtils.groupIndexDetails(Collections.emptyList(), entry -> entry.issuer).isEmpty());
    }

    @Test
    public void preservesNullKeysInTemporaryGroups() {
        ArbitraryDataIndexDetail first = detail(null, "alice", 1);
        ArbitraryDataIndexDetail second = detail("alpha", null, 2);
        ArbitraryDataIndexDetail third = detail(null, null, 3);
        List<ArbitraryDataIndexDetail> input = List.of(first, second, third);

        Map<String, List<ArbitraryDataIndexDetail>> byTerm =
                ArbitraryIndexUtils.groupIndexDetails(input, entry -> entry.term);
        Map<String, List<ArbitraryDataIndexDetail>> byIssuer =
                ArbitraryIndexUtils.groupIndexDetails(input, entry -> entry.issuer);

        assertEquals(2, byTerm.size());
        assertEquals(List.of(first, third), byTerm.get(null));
        assertEquals(List.of(second), byTerm.get("alpha"));
        assertEquals(2, byIssuer.size());
        assertEquals(List.of(second, third), byIssuer.get(null));
        assertEquals(List.of(first), byIssuer.get("alice"));
    }

    @Test
    public void largeGroupsPreserveOrderAndRefreshesUseIndependentLists() {
        List<ArbitraryDataIndexDetail> input = new ArrayList<>();
        for (int i = 0; i < 12000; i++) {
            input.add(detail("alpha", "alice", i + 1));
        }
        for (boolean byTerm : new boolean[] {true, false}) {
            Map<String, List<ArbitraryDataIndexDetail>> first = ArbitraryIndexUtils.groupIndexDetails(
                    input, entry -> byTerm ? entry.term : entry.issuer);
            Map<String, List<ArbitraryDataIndexDetail>> second = ArbitraryIndexUtils.groupIndexDetails(
                    input, entry -> byTerm ? entry.term : entry.issuer);
            String key = byTerm ? "alpha" : "alice";

            assertEquals(1, first.size());
            assertEquals(input, first.get(key));
            assertEquals(first, second);
            assertNotSame(input, first.get(key));
            assertNotSame(first.get(key), second.get(key));
        }
    }

    private static ArbitraryDataIndexDetail detail(String term, String issuer, int rank) {
        return new ArbitraryDataIndexDetail(issuer, rank,
                new ArbitraryDataIndex(term, "resource", 1, "link"), "idx-test");
    }
}
