package org.qortal.arbitrary;

import org.qortal.arbitrary.ArbitraryDataFile.ResourceIdType;
import org.qortal.arbitrary.exception.MissingDataException;
import org.qortal.arbitrary.misc.Service;
import org.qortal.data.transaction.ArbitraryTransactionData;
import org.qortal.repository.DataException;
import org.qortal.repository.Repository;
import org.qortal.repository.RepositoryManager;
import org.qortal.utils.ArbitraryTransactionUtils;
import org.qortal.utils.NTP;

import java.io.IOException;

public class ArbitraryDataBuildQueueItem extends ArbitraryDataResource {

    private final long creationTimestamp;
    private volatile Long buildStartTimestamp;
    private volatile Long failureTimestamp;
    private int priority;
    private int sizePriority;
    private Long dataSize;

    private static final int HIGH_PRIORITY_THRESHOLD = 5;
    private static final long LARGE_FILE_SIZE = 200L * ArbitraryDataFile.CHUNK_SIZE;
    private static final long PRIORITY_AGING_INTERVAL_MS = 10_000L;

    /* Failed builds can be retried after five minutes. Live builds are never evicted by age. */
    public static long FAILURE_TIMEOUT = 5 * 60 * 1000L;

    public ArbitraryDataBuildQueueItem(String resourceId, ResourceIdType resourceIdType, Service service, String identifier) {
        super(resourceId, resourceIdType, service, identifier);
        Long now = NTP.getTime();
        this.creationTimestamp = now != null ? now : System.currentTimeMillis();
    }

    /**
     * Read scheduling metadata once, before insertion and outside the queue monitor.
     * Transaction size is available even before the file/chunk metadata has been downloaded.
     * Unknown sizes remain eligible for the regular workers.
     */
    public void initializeSchedulingMetadata() {
        try {
            this.dataSize = this.fetchDataSize();
        } catch (DataException e) {
            this.dataSize = null;
        }
        this.calculatePriority();
    }

    protected Long fetchDataSize() throws DataException {
        if (this.resourceIdType != ResourceIdType.NAME)
            return null;
        try (Repository repository = RepositoryManager.getRepository()) {
            byte[] signature = repository.getArbitraryRepository()
                    .getLatestSignature(this.service, this.resourceId, this.identifier);
            if (signature == null)
                return null;
            ArbitraryTransactionData transaction = repository.getArbitraryRepository()
                    .getSingleTransactionBySignature(signature);
            return transaction != null ? (long) transaction.getSize() : null;
        }
    }

    public void prepareForBuild() {
        Long now = NTP.getTime();
        this.prepareForBuild(now != null ? now : System.currentTimeMillis());
    }

    public void prepareForBuild(long now) {
        this.buildStartTimestamp = now;
    }

    public void build() throws IOException, DataException, MissingDataException {
        if (NTP.getTime() == null)
            throw new DataException("NTP time hasn't synced yet");

        ArbitraryDataReader reader = new ArbitraryDataReader(
                this.resourceId, this.resourceIdType, this.service, this.identifier);
        try {
            reader.loadSynchronously(true, 0L);
        } finally {
            ArbitraryTransactionUtils.getStatus(service, resourceId, identifier, false, true);
        }
    }

    public boolean isBuilding() {
        return this.buildStartTimestamp != null;
    }

    public boolean isQueued() {
        return this.buildStartTimestamp == null;
    }

    public boolean hasReachedFailureTimeout(Long now) {
        return now != null && this.failureTimestamp != null && now - this.failureTimestamp > FAILURE_TIMEOUT;
    }

    public Long getBuildStartTimestamp() {
        return this.buildStartTimestamp;
    }

    /** Size is an additive scheduling preference; explicit caller priority is preserved. */
    public void calculatePriority() {
        if (this.dataSize == null || this.dataSize < 0) {
            this.sizePriority = 0;
        } else if (this.dataSize <= 5L * ArbitraryDataFile.CHUNK_SIZE) {
            this.sizePriority = 10;
        } else if (this.dataSize <= 20L * ArbitraryDataFile.CHUNK_SIZE) {
            this.sizePriority = 5;
        } else if (this.dataSize <= 100L * ArbitraryDataFile.CHUNK_SIZE) {
            this.sizePriority = 0;
        } else {
            this.sizePriority = -5;
        }
    }

    public long getEffectivePriority() {
        Long now = NTP.getTime();
        return this.getEffectivePriority(now != null ? now : System.currentTimeMillis());
    }

    /** Higher scores win. Evaluate every candidate using the same time snapshot. */
    public long getEffectivePriority(long now) {
        long ageBoost = Math.max(0L, now - this.creationTimestamp) / PRIORITY_AGING_INTERVAL_MS;
        return (long) this.priority + this.sizePriority + ageBoost;
    }

    public int getPriority() {
        return this.priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority != null ? priority : 0;
    }

    public boolean isHighPriority() {
        return this.priority >= HIGH_PRIORITY_THRESHOLD;
    }

    public boolean isLargeFile() {
        return this.dataSize != null && this.dataSize > LARGE_FILE_SIZE;
    }

    public void setFailed(boolean failed) {
        Long now = NTP.getTime();
        this.failureTimestamp = failed ? (now != null ? now : System.currentTimeMillis()) : null;
    }
}
