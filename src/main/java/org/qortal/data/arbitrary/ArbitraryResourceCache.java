package org.qortal.data.arbitrary;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ArbitraryResourceCache {
    /**
     * service.value -> (name + "\0" + identifier) -> resource data
     *
     * The inner Map is accessed only while holding a lock on the outer ConcurrentHashMap
     * instance (i.e. synchronized (dataByService) { ... }).
     */
    private ConcurrentHashMap<Integer, Map<String, ArbitraryResourceData>> dataByService = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Integer> levelByName = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, ServiceSearchIndex> searchIndexes = new ConcurrentHashMap<>();
    private volatile boolean initialized;
    // Accessed only under dataByService, including updates recorded by put().
    private final List<Refresh> refreshes = new ArrayList<>();

    private ArbitraryResourceCache() {}

    private static ArbitraryResourceCache SINGLETON = new ArbitraryResourceCache();

    public static ArbitraryResourceCache getInstance(){
        return SINGLETON;
    }

    public ConcurrentHashMap<String, Integer> getLevelByName() {
        return levelByName;
    }

    public ConcurrentHashMap<Integer, Map<String, ArbitraryResourceData>> getDataByService() {
        return this.dataByService;
    }

    public boolean isInitialized() {
        return this.initialized;
    }

    /**
     * Start before reading the database snapshot. Committed puts during the read
     * are replayed when publishing it, so an older snapshot cannot undo them.
     * Always close the refresh, including when the database read fails.
     */
    public Refresh beginRefresh() {
        synchronized (this.dataByService) {
            Refresh refresh = new Refresh();
            this.refreshes.add(refresh);
            return refresh;
        }
    }

    public final class Refresh implements AutoCloseable {
        private final Map<Integer, Map<String, ArbitraryResourceData>> updates = new HashMap<>();
        private boolean closed;

        private Refresh() {}

        public void replaceAll(Map<Integer, Map<String, ArbitraryResourceData>> resourcesByService) {
            synchronized (dataByService) {
                if (this.closed)
                    throw new IllegalStateException("Refresh already closed");
                Map<Integer, Map<String, ArbitraryResourceData>> merged = new HashMap<>(resourcesByService);
                for (Map.Entry<Integer, Map<String, ArbitraryResourceData>> entry : this.updates.entrySet()) {
                    Map<String, ArbitraryResourceData> serviceResources = new HashMap<>(
                            merged.getOrDefault(entry.getKey(), java.util.Collections.emptyMap()));
                    serviceResources.putAll(entry.getValue());
                    merged.put(entry.getKey(), serviceResources);
                }
                ArbitraryResourceCache.this.replaceAll(merged);
                close();
            }
        }

        @Override
        public void close() {
            synchronized (dataByService) {
                this.closed = true;
                refreshes.remove(this);
                this.updates.clear();
            }
        }
    }

    public void replaceAll(Map<Integer, Map<String, ArbitraryResourceData>> resourcesByService) {
        synchronized (this.dataByService) {
            for (Map.Entry<Integer, Map<String, ArbitraryResourceData>> entry : resourcesByService.entrySet()) {
                ServiceSearchIndex index = this.searchIndexes.get(entry.getKey());
                if (index == null)
                    index = new ServiceSearchIndex();
                index.replaceAll(entry.getValue());
                // Publish a new service only after its index is complete. Existing
                // services are reconciled under the same lock used by searches.
                this.searchIndexes.put(entry.getKey(), index);
            }
            this.searchIndexes.keySet().retainAll(resourcesByService.keySet());
            this.dataByService.clear();
            this.dataByService.putAll(resourcesByService);
            this.initialized = true;
        }
    }

    public void put(ArbitraryResourceData data) {
        if (data == null || data.service == null || data.name == null)
            return;
        String key = resourceKey(data.name, data.identifier);
        synchronized (this.dataByService) {
            this.dataByService.computeIfAbsent(data.service.value, ignored -> new LinkedHashMap<>()).put(key, data);
            this.searchIndexes.computeIfAbsent(data.service.value, ignored -> new ServiceSearchIndex()).put(key, data);
            for (Refresh refresh : this.refreshes)
                refresh.updates.computeIfAbsent(data.service.value, ignored -> new HashMap<>()).put(key, data);
        }
    }

    /** Treat returned candidates as read-only; later cache writes do not change their membership. */
    public List<ArbitraryResourceData> indexedCandidates(int service, String query, String identifier,
                                                          List<String> names, String title, String description,
                                                          boolean prefixOnly, List<String> exactNames,
                                                          List<String> keywords, boolean defaultResource) {
        ServiceSearchIndex index = this.searchIndexes.get(service);
        if (index == null)
            return new ArrayList<>();
        return index.candidates(query, identifier, names, title, description,
                prefixOnly, exactNames, keywords, defaultResource);
    }

    /** Compose the inner-map key from name and identifier. */
    public static String resourceKey(String name, String identifier) {
        return name + "\0" + (identifier != null ? identifier : "default");
    }

    private static final class ServiceSearchIndex {
        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        private final Object snapshotLock = new Object();
        private volatile List<ArbitraryResourceData> allCandidates;
        private Map<String, IndexedResource> byResourceKey = new LinkedHashMap<>();
        private final NavigableMap<String, IndexedResource> names = new TreeMap<>();
        private final NavigableMap<String, IndexedResource> identifiers = new TreeMap<>();
        private final NavigableMap<String, IndexedResource> titles = new TreeMap<>();
        private final NavigableMap<String, IndexedResource> descriptions = new TreeMap<>();

        private void replaceAll(Map<String, ArbitraryResourceData> resources) {
            this.lock.writeLock().lock();
            try {
                this.allCandidates = null;
                Map<String, IndexedResource> replacement = new LinkedHashMap<>();
                List<IndexedResource> removals = new ArrayList<>();
                List<IndexedResource> additions = new ArrayList<>();
                // Preserve incoming iteration order (including sort tie behavior), while
                // retaining the expensive normalized keys and tree nodes when possible.
                for (Map.Entry<String, ArbitraryResourceData> entry : resources.entrySet()) {
                    String key = entry.getKey();
                    ArbitraryResourceData data = entry.getValue();
                    IndexedResource previous = this.byResourceKey.get(key);
                    if (previous != null && previous.matchesIndexedFields(data)) {
                        previous.updateData(data);
                        replacement.put(key, previous);
                    } else {
                        if (previous != null)
                            removals.add(previous);
                        IndexedResource current = new IndexedResource(key, data);
                        additions.add(current);
                        replacement.put(key, current);
                    }
                }
                for (Map.Entry<String, IndexedResource> entry : this.byResourceKey.entrySet())
                    if (!replacement.containsKey(entry.getKey()))
                        removals.add(entry.getValue());

                // With widespread changes, clearing/repopulating the trees needs fewer
                // edits than individually removing and inserting most of their entries.
                if ((long) removals.size() + additions.size() > replacement.size()) {
                    this.names.clear();
                    this.identifiers.clear();
                    this.titles.clear();
                    this.descriptions.clear();
                    for (IndexedResource current : replacement.values())
                        current.addTo(this);
                } else {
                    for (IndexedResource previous : removals)
                        previous.removeFrom(this);
                    for (IndexedResource current : additions)
                        current.addTo(this);
                }
                this.byResourceKey = replacement;
            } finally {
                this.lock.writeLock().unlock();
            }
        }

        private void put(String resourceKey, ArbitraryResourceData data) {
            this.lock.writeLock().lock();
            try {
                this.allCandidates = null;
                IndexedResource previous = this.byResourceKey.remove(resourceKey);
                if (previous != null)
                    previous.removeFrom(this);
                IndexedResource current = new IndexedResource(resourceKey, data);
                this.byResourceKey.put(resourceKey, current);
                current.addTo(this);
            } finally {
                this.lock.writeLock().unlock();
            }
        }

        private List<ArbitraryResourceData> candidates(String query, String identifier,
                                                                     List<String> requestedNames,
                                                                     String title, String description,
                                                                     boolean prefixOnly,
                                                                     List<String> exactNames,
                                                                     List<String> keywords,
                                                                     boolean defaultResource) {
            this.lock.readLock().lock();
            try {
                LinkedHashMap<String, ArbitraryResourceData> selected = null;

                if (exactNames != null && !exactNames.isEmpty()) {
                    LinkedHashMap<String, ArbitraryResourceData> matches = new LinkedHashMap<>();
                    for (String exactName : exactNames)
                        addPrefix(this.names, normalize(exactName) + '\0', matches);
                    selected = intersect(selected, matches);
                }

                if (prefixOnly && requestedNames != null && !requestedNames.isEmpty()) {
                    LinkedHashMap<String, ArbitraryResourceData> matches = new LinkedHashMap<>();
                    for (String requestedName : requestedNames)
                        addPrefix(this.names, normalize(requestedName), matches);
                    selected = intersect(selected, matches);
                }

                if (prefixOnly && query != null && !query.isEmpty()) {
                    LinkedHashMap<String, ArbitraryResourceData> matches = new LinkedHashMap<>();
                    String normalized = normalize(query);
                    addPrefix(this.names, normalized, matches);
                    if (!defaultResource) {
                        addPrefix(this.identifiers, normalized, matches);
                        addPrefix(this.titles, normalized, matches);
                        addPrefix(this.descriptions, normalized, matches);
                    }
                    selected = intersect(selected, matches);
                }

                if (prefixOnly && identifier != null && !identifier.isEmpty())
                    selected = intersect(selected, prefix(this.identifiers, normalize(identifier)));
                if (prefixOnly && title != null && !title.isEmpty())
                    selected = intersect(selected, prefix(this.titles, normalize(title)));
                if (prefixOnly && description != null && !description.isEmpty())
                    selected = intersect(selected, prefix(this.descriptions, normalize(description)));

                if (!prefixOnly && query != null && !query.isEmpty()) {
                    String term = normalize(query);
                    LinkedHashMap<String, ArbitraryResourceData> matches = new LinkedHashMap<>();
                    for (Map.Entry<String, IndexedResource> entry : this.byResourceKey.entrySet()) {
                        IndexedResource resource = entry.getValue();
                        if (resource.nameKey.contains(term) || (!defaultResource
                                && (contains(resource.identifierKey, term) || contains(resource.titleKey, term)
                                || contains(resource.descriptionKey, term))))
                            matches.put(entry.getKey(), resource.data);
                    }
                    selected = intersect(selected, matches);
                }

                if (!prefixOnly && identifier != null && !identifier.isEmpty())
                    selected = intersect(selected, containing(this.byResourceKey, normalize(identifier), 1));
                if (!prefixOnly && title != null && !title.isEmpty())
                    selected = intersect(selected, containing(this.byResourceKey, normalize(title), 2));
                if (!prefixOnly && description != null && !description.isEmpty())
                    selected = intersect(selected, containing(this.byResourceKey, normalize(description), 3));
                if (keywords != null && !keywords.isEmpty()) {
                    LinkedHashMap<String, ArbitraryResourceData> matches = new LinkedHashMap<>();
                    for (Map.Entry<String, IndexedResource> entry : this.byResourceKey.entrySet()) {
                        for (String keyword : keywords) {
                            if (contains(entry.getValue().descriptionKey, normalize(keyword))) {
                                matches.put(entry.getKey(), entry.getValue().data);
                                break;
                            }
                        }
                    }
                    selected = intersect(selected, matches);
                }

                if (selected == null)
                    return allCandidates();
                return new ArrayList<>(selected.values());
            } finally {
                this.lock.readLock().unlock();
            }
        }

        // Called under the read lock. Build once per update, rather than copying every
        // resource on every broad search. Only snapshot creation serializes readers;
        // writers cannot change the entries until all readers release the read lock.
        private List<ArbitraryResourceData> allCandidates() {
            List<ArbitraryResourceData> snapshot = this.allCandidates;
            if (snapshot == null) {
                synchronized (this.snapshotLock) {
                    snapshot = this.allCandidates;
                    if (snapshot == null) {
                        List<ArbitraryResourceData> values = new ArrayList<>(this.byResourceKey.size());
                        for (IndexedResource resource : this.byResourceKey.values())
                            values.add(resource.data);
                        snapshot = Collections.unmodifiableList(values);
                        this.allCandidates = snapshot;
                    }
                }
            }
            return snapshot;
        }

        private static LinkedHashMap<String, ArbitraryResourceData> containing(
                Map<String, IndexedResource> resources, String term, int field) {
            LinkedHashMap<String, ArbitraryResourceData> matches = new LinkedHashMap<>();
            for (Map.Entry<String, IndexedResource> entry : resources.entrySet()) {
                IndexedResource resource = entry.getValue();
                String value = field == 1 ? resource.identifierKey
                        : field == 2 ? resource.titleKey : resource.descriptionKey;
                if (contains(value, term))
                    matches.put(entry.getKey(), resource.data);
            }
            return matches;
        }

        private static boolean contains(String value, String term) {
            return value != null && value.contains(term);
        }

        private static LinkedHashMap<String, ArbitraryResourceData> prefix(
                NavigableMap<String, IndexedResource> index, String prefix) {
            LinkedHashMap<String, ArbitraryResourceData> matches = new LinkedHashMap<>();
            addPrefix(index, prefix, matches);
            return matches;
        }

        private static void addPrefix(NavigableMap<String, IndexedResource> index, String prefix,
                                      Map<String, ArbitraryResourceData> destination) {
            if (prefix == null)
                return;
            for (Map.Entry<String, IndexedResource> entry :
                    index.subMap(prefix, true, prefix + Character.MAX_VALUE, true).entrySet()) {
                ArbitraryResourceData data = entry.getValue().data;
                destination.put(resourceKey(data.name, data.identifier), data);
            }
        }

        private static LinkedHashMap<String, ArbitraryResourceData> intersect(
                LinkedHashMap<String, ArbitraryResourceData> current,
                LinkedHashMap<String, ArbitraryResourceData> matches) {
            if (current == null)
                return matches;
            current.keySet().retainAll(matches.keySet());
            return current;
        }

        private static String normalize(String value) {
            return value == null ? null : value.toLowerCase(Locale.ROOT);
        }
    }

    private static final class IndexedResource {
        // All access is under the owning ServiceSearchIndex read/write lock. Tree nodes
        // point to this wrapper so refreshing non-indexed fields needs no tree edits.
        private ArbitraryResourceData data;
        private String name;
        private String identifier;
        private String title;
        private String description;
        private final String nameKey;
        private final String identifierKey;
        private final String titleKey;
        private final String descriptionKey;

        private IndexedResource(String resourceKey, ArbitraryResourceData data) {
            updateData(data);
            String suffix = "\0" + resourceKey;
            this.nameKey = normalize(data.name) + suffix;
            this.identifierKey = data.identifier == null ? null : normalize(data.identifier) + suffix;
            this.titleKey = data.metadata == null || data.metadata.getTitle() == null ? null
                    : normalize(data.metadata.getTitle()) + suffix;
            this.descriptionKey = data.metadata == null || data.metadata.getDescription() == null ? null
                    : normalize(data.metadata.getDescription()) + suffix;
        }

        private void updateData(ArbitraryResourceData data) {
            this.data = data;
            // Remember the indexed values independently of mutable resource data,
            // but use fresh references so previous database rows can be collected.
            this.name = data.name;
            this.identifier = data.identifier;
            this.title = data.metadata == null ? null : data.metadata.getTitle();
            this.description = data.metadata == null ? null : data.metadata.getDescription();
        }

        private boolean matchesIndexedFields(ArbitraryResourceData data) {
            return Objects.equals(this.name, data.name)
                    && Objects.equals(this.identifier, data.identifier)
                    && Objects.equals(this.title, data.metadata == null ? null : data.metadata.getTitle())
                    && Objects.equals(this.description, data.metadata == null ? null : data.metadata.getDescription());
        }

        private void addTo(ServiceSearchIndex index) {
            index.names.put(this.nameKey, this);
            if (this.identifierKey != null) index.identifiers.put(this.identifierKey, this);
            if (this.titleKey != null) index.titles.put(this.titleKey, this);
            if (this.descriptionKey != null) index.descriptions.put(this.descriptionKey, this);
        }

        private void removeFrom(ServiceSearchIndex index) {
            index.names.remove(this.nameKey);
            if (this.identifierKey != null) index.identifiers.remove(this.identifierKey);
            if (this.titleKey != null) index.titles.remove(this.titleKey);
            if (this.descriptionKey != null) index.descriptions.remove(this.descriptionKey);
        }

        private static String normalize(String value) {
            return value.toLowerCase(Locale.ROOT);
        }
    }
}
