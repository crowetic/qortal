package org.qortal.test.arbitrary;

import org.junit.Before;
import org.junit.Test;
import org.qortal.arbitrary.misc.Service;
import org.qortal.data.arbitrary.ArbitraryResourceData;
import org.qortal.data.arbitrary.ArbitraryResourceStatus.Status;
import org.qortal.repository.Repository;
import org.qortal.repository.RepositoryManager;
import org.qortal.test.common.Common;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class ArbitraryResourceStatusPersistenceTests extends Common {

    @Before
    public void beforeTest() throws Exception {
        Common.useDefaultSettings();
    }

    @Test
    public void statusUpdateOnlyChangesDifferentStoredValue() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            ArbitraryResourceData resource = new ArbitraryResourceData(Service.WEBSITE,
                    "status-persistence-test", null);
            resource.size = 123;
            resource.created = System.currentTimeMillis();
            resource.latestSignature = new byte[64];
            repository.getArbitraryRepository().save(resource);
            repository.saveChanges();

            // New cache rows default to PUBLISHED.
            assertFalse(repository.getArbitraryRepository().setStatus(resource, Status.PUBLISHED));
            repository.saveChanges();

            assertTrue(repository.getArbitraryRepository().setStatus(resource, Status.DOWNLOADED));
            repository.saveChanges();

            assertFalse(repository.getArbitraryRepository().setStatus(resource, Status.DOWNLOADED));
            repository.saveChanges();

            ArbitraryResourceData stored = repository.getArbitraryRepository()
                    .getArbitraryResource(Service.WEBSITE, resource.name, null);
            assertNotNull(stored);
            assertNotNull(stored.status);
            assertEquals(Status.DOWNLOADED, stored.status.getStatus());
        }
    }
}
