package org.qortal.repository.hsqldb;

import org.qortal.utils.ByteArray;

import java.util.LinkedHashMap;
import java.util.Map;

/** Only immutable bytes, never database existence or mutable chain state. */
final class ATImmutableCache {
    private final long limit;
    private long size;
    private final Map<ByteArray, byte[]> entries = new LinkedHashMap<>(128, 0.75f, true);

    ATImmutableCache(long limit) { this.limit = limit; }
    synchronized byte[] get(byte[] hash) {
        byte[] value = entries.get(ByteArray.wrap(hash));
        return value == null ? null : value.clone();
    }
    synchronized void put(byte[] hash, byte[] value) {
        if (value == null || value.length + 128L > limit) return;
        ByteArray key = ByteArray.copyOf(hash);
        byte[] previous = entries.put(key, value.clone());
        if (previous != null) size -= previous.length + 128L;
        size += value.length + 128L;
        java.util.Iterator<byte[]> iterator = entries.values().iterator();
        while (size > limit && iterator.hasNext()) { size -= iterator.next().length + 128L; iterator.remove(); }
    }
}
