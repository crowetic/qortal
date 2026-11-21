package org.qortal.repository.hsqldb;

import java.lang.reflect.*;
import java.nio.file.*;
import java.sql.*;
import org.qortal.repository.*;
import org.qortal.test.common.*;
import org.qortal.utils.NTP;

/** Child-process crash probe using a disposable file-backed test blockchain. */
public final class ATCrashProbe {
    private static String fingerprint(Connection connection) throws Exception {
        java.security.MessageDigest digest=java.security.MessageDigest.getInstance("SHA-256");
        for(String table:new String[]{"Accounts","AccountBalances","ATDefinitions","ATCode","ATCurrentState","ATBlockPayloads","ATRetainedStatePayloads","Transactions","ATRecipientTransactions","Blocks","ATStoreInfo","ATBlockResults"}) {
            digest.update(table.getBytes(java.nio.charset.StandardCharsets.UTF_8));
			String order=(table.equals("ATBlockResults") || table.equals("ATBlockPayloads")) ? " ORDER BY 1" : " ORDER BY 1,2";
            try(Statement statement=connection.createStatement();ResultSet rs=statement.executeQuery("SELECT * FROM "+table+order)) {
                while(rs.next()) for(int column=1;column<=rs.getMetaData().getColumnCount();++column) {
                    Object value=rs.getObject(column);
                    byte[] bytes=value==null ? new byte[]{0} : value instanceof byte[] ? (byte[])value : value.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    digest.update(java.nio.ByteBuffer.allocate(4).putInt(bytes.length).array());digest.update(bytes);
                }
            }
        }
        return com.google.common.hash.HashCode.fromBytes(digest.digest()).toString();
    }
    public static void main(String[] args) {
        try { run(args); } catch (Throwable failure) { failure.printStackTrace(); System.exit(1); }
    }
    private static void run(String[] args) throws Exception {
        Path directory=Paths.get(args[0]).toRealPath();
        if (!Files.isRegularFile(directory.resolve("DISPOSABLE_AT_REPLAY"))) throw new IllegalArgumentException("Missing marker");
        String stage=args[1];
        // Load only test settings and crypto providers, then open the explicitly named file DB.
        Class.forName(Common.class.getName());
        NTP.setFixedOffset(0L);
        RepositoryFactory factory=new HSQLDBRepositoryFactory("jdbc:hsqldb:file:"+directory.resolve("blockchain")+";create=true;hsqldb.full_log_replay=true");
        RepositoryManager.setRepositoryFactory(factory);
        try (HSQLDBRepository r=(HSQLDBRepository)factory.getRepository(); Statement mode=r.connection.createStatement()) {
            mode.execute("SET FILES LOG TRUE");
            mode.execute("SET FILES WRITE DELAY 0");
            mode.execute("CHECKPOINT");
        }
        if (stage.equals("init") || stage.equals("init_frozen") || stage.equals("init_halt") || stage.equals("init_orphan")) {
            Common.resetBlockchain();
            try (Repository r=factory.getRepository()) {
                String address=AtUtils.doDeployAT(r,Common.getTestAccount(r,"alice"),AtUtils.buildSimpleAT(),1_00000000L).getATAccount().getAddress();
                if(stage.equals("init_frozen")) {
                    r.getAccountRepository().setAssetBalances(java.util.Collections.singletonList(new org.qortal.data.account.AccountBalanceData(address,0,0)));
                    r.saveChanges();BlockUtils.mintBlock(r);
                    if(!r.getATRepository().fromATAddress(address).getIsFrozen())throw new AssertionError("Fixture must be frozen");
                }
                if(stage.equals("init_orphan")) BlockUtils.mintBlock(r);
                org.qortal.account.PrivateKeyAccount alice=Common.getTestAccount(r,"alice");
                org.qortal.data.transaction.BaseTransactionData base=new org.qortal.data.transaction.BaseTransactionData(
                    System.currentTimeMillis(),0,alice.getLastReference(),alice.getPublicKey(),1_00000000L,null);
                TransactionUtils.signAndImportValid(r,new org.qortal.data.transaction.PaymentTransactionData(base,address,1_00000000L),alice);
                r.saveChanges();
                Files.writeString(directory.resolve("height.txt"),String.valueOf(r.getBlockRepository().getBlockchainHeight()));
                Files.writeString(directory.resolve("fingerprint.txt"),fingerprint(((HSQLDBRepository)r).connection));
                if(stage.equals("init_halt")) Runtime.getRuntime().halt(83);
            }
        } else if (stage.equals("check")) {
            try (HSQLDBRepository r=(HSQLDBRepository)factory.getRepository()) {
                HSQLDBATStoreMigration.validateActiveStore(r.connection);
                int height=r.getBlockRepository().getBlockchainHeight();
                int original=Integer.parseInt(Files.readString(directory.resolve("height.txt")));
                boolean committed=Files.exists(directory.resolve("expect-committed"));
                int change=committed?Integer.parseInt(Files.readString(directory.resolve("expect-committed"))):0;
                if (height!=original+change) throw new AssertionError("Unexpected recovered height "+height);
                String expected=Files.readString(directory.resolve(committed?"committed-fingerprint.txt":"fingerprint.txt"));
                if(!expected.equals(fingerprint(r.connection))) throw new AssertionError("Recovered ledger differs from atomic expected state");
                System.out.println("RECOVERY_OK height="+height+" ledger fingerprint matches");
            }
        } else {
            try (HSQLDBRepository r=(HSQLDBRepository)factory.getRepository()) {
                Connection original=r.connection;
                boolean orphan=stage.startsWith("orphan_");
                String boundary=orphan?stage.substring("orphan_".length()):stage;
                int expectedHeight=Integer.parseInt(Files.readString(directory.resolve("height.txt")))+(orphan?-1:1);
                r.connection=(Connection)Proxy.newProxyInstance(ATCrashProbe.class.getClassLoader(),new Class<?>[]{Connection.class},(p,m,a)->{
                    boolean blockCommit=false;
                    if(m.getName().equals("commit")) try(Statement query=original.createStatement();ResultSet rs=query.executeQuery("SELECT MAX(height) FROM Blocks")) { rs.next();blockCommit=rs.getInt(1)==expectedHeight; }
                    if (blockCommit && boundary.equals("commit_before")) Runtime.getRuntime().halt(83);
                    Object result;
                    try { result=m.invoke(original,a); } catch (InvocationTargetException e) { throw e.getCause(); }
                    if (blockCommit && boundary.equals("commit_after")) {
                        Files.writeString(directory.resolve("committed-fingerprint.txt"),fingerprint(original));
                        Files.writeString(directory.resolve("expect-committed"),orphan?"-1":"1"); Runtime.getRuntime().halt(83);
                    }
                    if (m.getName().equals("prepareStatement")) {
                        String sql=(String)a[0]; PreparedStatement statement=(PreparedStatement)result;
                        return Proxy.newProxyInstance(ATCrashProbe.class.getClassLoader(),new Class<?>[]{PreparedStatement.class},(pp,mm,aa)->{
                            Object value;
                            try { value=mm.invoke(statement,aa); } catch (InvocationTargetException e) { throw e.getCause(); }
                            boolean executed=mm.getName().startsWith("execute");
							boolean hit=(boundary.equals("payload")&&sql.startsWith("INSERT INTO ATBlockPayloads("))
									||(boundary.equals("history")&&sql.startsWith(orphan?"DELETE FROM ATBlockResults ":"INSERT INTO ATBlockResults("))
									||(boundary.equals("head")&&(sql.startsWith("UPDATE ATCurrentState SET latest_state_height")
									||sql.startsWith("UPDATE ATCurrentState SET previous_state_height")))
                                    ||(boundary.equals("account")&&sql.startsWith("UPDATE AccountBalances"))
                                    ||(boundary.equals("recipient")&&sql.startsWith("INSERT INTO ATRecipientTransactions"))
                                    ||(boundary.equals("marker")&&sql.startsWith("UPDATE ATStoreInfo SET tip_signature"));
                            if (executed&&hit) Runtime.getRuntime().halt(83);
                            return value;
                        });
                    }
                    return result;
                });
                if(orphan) {
                    new org.qortal.block.Block(r,r.getBlockRepository().getLastBlock()).orphan();r.saveChanges();
                } else BlockUtils.mintBlock(r);
                throw new AssertionError("Crash boundary not reached: "+stage);
            }
        }
        RepositoryManager.closeRepositoryFactory();
        System.exit(0);
    }
}
