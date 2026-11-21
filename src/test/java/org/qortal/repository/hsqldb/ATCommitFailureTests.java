package org.qortal.repository.hsqldb;

import org.junit.Before;
import org.junit.Test;
import org.qortal.repository.*;
import org.qortal.test.common.Common;
import java.lang.reflect.*;
import java.sql.*;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.Assert.*;

public class ATCommitFailureTests extends Common {
    @Before public void setup() throws DataException { Common.useDefaultSettings(); }

    @Test public void corruptDuplicatePayloadAbortsWrite() throws Exception {
        try (HSQLDBRepository r = (HSQLDBRepository)RepositoryManager.getRepository()) {
            String address = org.qortal.test.common.AtUtils.doDeployAT(r,Common.getTestAccount(r,"alice"),org.qortal.test.common.AtUtils.buildSimpleAT(),1_00000000L).getATAccount().getAddress();
            org.qortal.data.at.ATStateData state = r.getATRepository().getLatestATState(address);
			r.executeCheckedUpdate("UPDATE ATBlockPayloads SET payload_bytes=? WHERE block_height=?",new byte[]{42},state.getHeight());
			org.qortal.data.at.ATStateData deploymentState = new org.qortal.data.at.ATStateData(address,
					state.getHeight(), state.getStateData(), state.getStateHash(), state.getFees(), true,
					state.getSleepUntilMessageTimestamp());
			try { r.getATRepository().saveInitialState(deploymentState); fail("corrupt duplicate must fail"); }
            catch (DataException expected) { assertNotNull(expected.getMessage()); }
            r.discardChanges();
            assertArrayEquals(state.getStateData(),r.getATRepository().getLatestATState(address).getStateData());
            r.discardChanges();
        }
    }

    @Test public void ambiguousCommitDoesNotPublishAndPoisonsSession() throws Exception {
        try (HSQLDBRepository repository = (HSQLDBRepository) RepositoryManager.getRepository()) {
            Connection original = repository.connection;
            AtomicBoolean published = new AtomicBoolean();
            repository.afterCommit(() -> published.set(true));
            repository.connection = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{Connection.class},(proxy,method,args)->{
                try {
                    Object result = method.invoke(original,args);
                    if (method.getName().equals("commit")) throw new SQLException("simulated uncertain commit result");
                    return result;
                } catch (InvocationTargetException e) { throw e.getCause(); }
            });
            try {
                try { repository.saveChanges(); fail("commit should fail"); } catch (DataException expected) { }
                assertFalse(published.get());
                try { repository.prepareStatement("VALUES(1)"); fail("session must be rejected"); } catch (SQLException expected) { }
            } finally { repository.connection = original; repository.discardChanges(); }
        }
    }
}
