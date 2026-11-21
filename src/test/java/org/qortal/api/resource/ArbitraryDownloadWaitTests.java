package org.qortal.api.resource;

import org.junit.Before;
import org.junit.Test;
import org.qortal.arbitrary.ArbitraryDataReader;
import org.qortal.arbitrary.ArbitraryDataFile.ResourceIdType;
import org.qortal.arbitrary.exception.MissingDataException;
import org.qortal.arbitrary.misc.Service;
import org.qortal.repository.DataException;
import org.qortal.test.common.Common;
import static org.junit.Assert.*;

public class ArbitraryDownloadWaitTests {
    @Before public void settings() throws Exception { Common.useDefaultSettings(); }

    private static class Reader extends ArbitraryDataReader {
        int calls;
        int failures;
        boolean rebuild;
        Reader(int failures) throws DataException {
            super("download-wait-test", ResourceIdType.FILE_HASH, Service.ARBITRARY_DATA, null);
            this.failures = failures;
        }
        @Override public boolean isBuilding() { throw new AssertionError("API must not poll isBuilding"); }
        @Override public void loadSynchronouslyWhenReady(boolean rebuild, java.util.function.BooleanSupplier stopping, long timeoutMillis) throws MissingDataException {
            this.rebuild = rebuild;
            if (++calls <= failures) throw new MissingDataException("missing");
        }
    }

    @Test public void preservesInitialAttemptAndRetryCount() throws Exception {
        Reader successful = new Reader(2);
        ArbitraryResource.loadForDownload(successful, true, 2, () -> false);
        assertEquals(3, successful.calls);
        assertTrue(successful.rebuild);
        Reader failed = new Reader(10);
        try {
            ArbitraryResource.loadForDownload(failed, false, 2, () -> false);
            fail("Expected missing data");
        } catch (MissingDataException expected) { assertEquals(3, failed.calls); }
    }

    @Test public void stoppingDoesNotStartAnotherLoad() throws Exception {
        Reader reader = new Reader(10);
        try {
            ArbitraryResource.loadForDownload(reader, false, 5, () -> reader.calls > 0);
            fail("Stopped request must not be reported as loaded");
        } catch (DataException expected) { assertTrue(expected.getMessage().contains("stopping")); }
        assertEquals(1, reader.calls);
    }
    @Test public void retryPacingSharesTheRequestDeadline() throws Exception {
        Reader reader = new Reader(10);
        try {
            ArbitraryResource.loadForDownload(reader, false, 5, () -> false, 50L);
            fail("Requested chunks cannot consume an unbounded retry wait");
        } catch (DataException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("Timed out"));
        }
        assertEquals(1, reader.calls);
    }

}
