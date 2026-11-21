package org.qortal.data.arbitrary;

import org.qortal.arbitrary.misc.Service;

/** Durable, coalesced work item for the rebuildable QDN resource cache. */
public class ArbitraryResourceDirtyData {

    public static final int HEAD_DIRTY = 1;
    public static final int METADATA_DIRTY = 1 << 1;
    public static final int STATUS_DIRTY = 1 << 2;
    public static final int RESOURCE_EXPIRED = 1 << 3;
    public static final int ALL_DIRTY = HEAD_DIRTY | METADATA_DIRTY | STATUS_DIRTY;

    private final Service service;
    private final String name;
    private final String identifier;
    private final int flags;
    private final byte[] triggeringSignature;
    private final long revision;
    private final int retryCount;

    public ArbitraryResourceDirtyData(Service service, String name, String identifier, int flags,
                                      byte[] triggeringSignature, long revision, int retryCount) {
        this.service = service;
        this.name = name;
        this.identifier = identifier;
        this.flags = flags;
        this.triggeringSignature = triggeringSignature;
        this.revision = revision;
        this.retryCount = retryCount;
    }

    public Service getService() { return service; }
    public String getName() { return name; }
    public String getIdentifier() { return identifier; }
    public int getFlags() { return flags; }
    public byte[] getTriggeringSignature() { return triggeringSignature; }
    public long getRevision() { return revision; }
    public int getRetryCount() { return retryCount; }
    public boolean hasFlag(int flag) { return (flags & flag) != 0; }
}
