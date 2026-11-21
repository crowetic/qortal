package org.qortal.test.repository;

import org.junit.After;
import org.junit.Test;
import org.qortal.arbitrary.misc.Service;
import org.qortal.data.arbitrary.ArbitraryResourceCache;
import org.qortal.data.arbitrary.ArbitraryResourceData;
import org.qortal.data.arbitrary.ArbitraryResourceMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ArbitraryResourceCacheIndexTests {

    private final ArbitraryResourceCache cache = ArbitraryResourceCache.getInstance();

    @After
    public void clearCache() {
        this.cache.replaceAll(new HashMap<>());
    }

    @Test
    public void prefixIndexPreservesNameIdentifierAndMetadataMatches() {
        ArbitraryResourceData nameMatch = resource("AlphaName", "default", "Other", "Other");
        ArbitraryResourceData identifierMatch = resource("OtherName", "alpha-id", "Other", "Other");
        ArbitraryResourceData titleMatch = resource("ThirdName", "default", "Alpha title", "Other");
        ArbitraryResourceData descriptionMatch = resource("FourthName", "default", "Other", "Alpha description");
        ArbitraryResourceData miss = resource("Miss", "default", "Other", "Other");
        replace(nameMatch, identifierMatch, titleMatch, descriptionMatch, miss);

        List<ArbitraryResourceData> candidates = this.cache.indexedCandidates(Service.WEBSITE.value,
                "ALPHA", null, null, null, null, true, null, null, false);

        assertEquals(4, candidates.size());
        assertTrue(candidates.contains(nameMatch));
        assertTrue(candidates.contains(identifierMatch));
        assertTrue(candidates.contains(titleMatch));
        assertTrue(candidates.contains(descriptionMatch));
    }

    @Test
    public void updatingResourceRemovesOldIndexEntries() {
        ArbitraryResourceData original = resource("Original", "default", "Old title", "Old description");
        replace(original);

        ArbitraryResourceData updated = resource("Original", "default", "New title", "New description");
        this.cache.put(updated);

        assertTrue(this.cache.indexedCandidates(Service.WEBSITE.value, null, null,
                null, "old", null, true, null, null, false).isEmpty());
        assertEquals(1, this.cache.indexedCandidates(Service.WEBSITE.value, null, null,
                null, "new", null, true, null, null, false).size());
    }

    @Test
    public void exactNameIndexDoesNotReturnLongerNames() {
        ArbitraryResourceData exact = resource("Name", "default", null, null);
        ArbitraryResourceData longer = resource("NameExtended", "default", null, null);
        replace(exact, longer);

        List<ArbitraryResourceData> candidates = this.cache.indexedCandidates(Service.WEBSITE.value,
                null, null, null, null, null, false, List.of("NAME"), null, false);
        assertEquals(1, candidates.size());
        assertEquals("Name", candidates.get(0).name);
    }

    @Test
    public void containsAndKeywordPrefilterPreserveMatches() {
        ArbitraryResourceData match = resource("SomeName", "topic-middle-value",
                "A useful title", "Contains RareKeyword in its description");
        ArbitraryResourceData miss = resource("Other", "default", "Other", "Nothing relevant");
        replace(match, miss);

        assertEquals(1, this.cache.indexedCandidates(Service.WEBSITE.value,
                "middle", null, null, null, null, false, null, null, false).size());
        assertEquals(1, this.cache.indexedCandidates(Service.WEBSITE.value,
                null, null, null, null, null, false, null,
                List.of("rarekeyword", "absent"), false).size());
    }

    private void replace(ArbitraryResourceData... resources) {
        Map<String, ArbitraryResourceData> serviceResources = new HashMap<>();
        for (ArbitraryResourceData resource : resources)
            serviceResources.put(ArbitraryResourceCache.resourceKey(resource.name, resource.identifier), resource);
        this.cache.replaceAll(Map.of(Service.WEBSITE.value, serviceResources));
    }

    private static ArbitraryResourceData resource(String name, String identifier,
                                                   String title, String description) {
        ArbitraryResourceData data = new ArbitraryResourceData(Service.WEBSITE, name, identifier);
        data.created = 1L;
        if (title != null || description != null) {
            data.metadata = new ArbitraryResourceMetadata();
            data.metadata.setTitle(title);
            data.metadata.setDescription(description);
        }
        return data;
    }
}
