package org.qortal.repository.hsqldb;

import org.junit.Before;
import org.junit.Test;
import org.qortal.crypto.Crypto;
import org.qortal.test.common.Common;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.UUID;
import static org.junit.Assert.*;

/** Exercise the real version-55 schema and the irreversible upgrade with live-shaped rows. */
public class IntegratedPerformanceMigrationTests {
    @Before public void settings() throws Exception { Common.useDefaultSettings(); }

    @Test public void upgrade55PreservesWalletFundingAndMigratesNonemptyAtPayloads() throws Exception {
        String url = "jdbc:hsqldb:mem:combined-upgrade-" + UUID.randomUUID();
        try (Connection c = DriverManager.getConnection(url); Statement s = c.createStatement()) {
            Method step = HSQLDBDatabaseUpdates.class.getDeclaredMethod("databaseUpdating", Connection.class, boolean.class);
            step.setAccessible(true);
            while (HSQLDBDatabaseUpdates.fetchDatabaseVersion(c) < 55) {
                assertTrue((boolean) step.invoke(null, c, true));
                s.executeUpdate("UPDATE DatabaseInfo SET version=version+1");
                c.commit();
            }
            byte[] code = {1,2,3}, payload = {4,5,6};
            try (PreparedStatement p = c.prepareStatement("INSERT INTO ATs "
                    + "(AT_address,creator,created_when,version,asset_id,code_bytes,code_hash,is_sleeping,is_finished,had_fatal_error,is_frozen) "
                    + "VALUES('fixture-at',?,100,2,0,?,?,FALSE,FALSE,FALSE,FALSE)")) {
                p.setBytes(1,new byte[32]); p.setBytes(2,code); p.setBytes(3,Crypto.digest(code)); p.executeUpdate();
            }
            try (PreparedStatement p = c.prepareStatement("INSERT INTO ATStates(AT_address,height,state_hash,fees,is_initial) "
                    + "VALUES('fixture-at',1,?,0,FALSE)")) {
                p.setBytes(1,Crypto.digest(payload)); p.executeUpdate();
            }
            try (PreparedStatement p = c.prepareStatement("INSERT INTO ATStatesData(AT_address,height,state_data) VALUES('fixture-at',1,?)")) {
                p.setBytes(1,payload); p.executeUpdate();
            }
            s.execute("INSERT INTO TradeBotStates(trade_private_key,acct_name,trade_state,trade_state_value,creator_address,updated_when,"
                    + "qort_amount,trade_native_public_key,trade_native_public_key_hash,trade_native_address,trade_foreign_public_key,"
                    + "trade_foreign_public_key_hash,foreign_amount,receiving_account_info,funding_fee,funding_output_index,funding_txid) "
                    + "VALUES(X'01','PirateChainACCTv3','ALICE_WAITING_FOR_AT_LOCK',20,'fixture-creator',100,100,X'02',X'03','fixture-native',"
                    + "X'04',X'05',200,X'06',10000,-1,'acknowledged-funding-id')");
            c.commit();
            HSQLDBDatabaseUpdates.updateDatabase(c);
            assertEquals(57,HSQLDBDatabaseUpdates.fetchDatabaseVersion(c));
            HSQLDBDatabaseUpdates.updateDatabase(c); // reopening must be idempotent
            try (ResultSet r = s.executeQuery("SELECT funding_fee,funding_output_index,funding_txid,receiving_account_info FROM TradeBotStates")) {
                assertTrue(r.next()); assertEquals(10000,r.getLong(1)); assertEquals(-1,r.getInt(2));
                assertEquals("acknowledged-funding-id",r.getString(3)); assertArrayEquals(new byte[]{6},r.getBytes(4));
                assertFalse(r.next());
            }
            try (ResultSet r = s.executeQuery("SELECT latest_state_height,payload_hash FROM ATCurrentState WHERE AT_address='fixture-at'")) {
                assertTrue(r.next()); assertEquals(1,r.getInt(1)); assertArrayEquals(Crypto.digest(payload),r.getBytes(2));
            }
            try (ResultSet r = s.executeQuery("SELECT payload_bytes FROM ATBlockPayloads WHERE block_height=1")) {
                assertTrue(r.next());
                assertArrayEquals(payload,org.qortal.at.ATStateBundle.deserialize(r.getBytes(1)).get("fixture-at").data);
            }
            s.execute("SHUTDOWN");
        }
    }
}
