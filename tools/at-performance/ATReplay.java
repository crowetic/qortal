import java.nio.file.*;
import java.security.Security;
import java.util.*;
import org.qortal.block.Block;
import org.qortal.controller.Controller;
import org.qortal.data.block.BlockData;
import org.qortal.repository.*;
import org.qortal.repository.hsqldb.HSQLDBRepositoryFactory;
import org.qortal.settings.Settings;
import org.qortal.transaction.Transaction;
import org.qortal.transform.block.*;
import org.qortal.utils.NTP;

/** Offline destructive replay tool. Only operates inside an explicitly marked disposable directory. */
public final class ATReplay {
    private static String ledgerFingerprint(Repository repository, int firstHeight) throws Exception {
        java.lang.reflect.Field field=Class.forName("org.qortal.repository.hsqldb.HSQLDBRepository").getDeclaredField("connection");
        field.setAccessible(true);
        java.sql.Connection connection=(java.sql.Connection)field.get(repository);
        java.security.MessageDigest digest=java.security.MessageDigest.getInstance("SHA-256");
        for(String table:new String[]{"Accounts","AccountBalances","ATDefinitions","ATCurrentState","ATCode","ATCurrentPayloads","ATRecipientTransactions","ATStoreInfo","ATBlockResults"}) {
            digest.update(table.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            String query;
            if (table.equals("ATCurrentPayloads")) {
                // Resolve logical current bytes through the repository, independent of
                // block-bundle versus retained-boundary physical placement.
                try (java.sql.Statement statement=connection.createStatement();
                     java.sql.ResultSet rs=statement.executeQuery("SELECT AT_address FROM ATCurrentState ORDER BY AT_address")) {
                    while (rs.next()) {
                        String address=rs.getString(1);
                        org.qortal.data.at.ATStateData state=repository.getATRepository().getLatestATState(address);
                        byte[] bytes=state.getStateData();
                        if (bytes==null) throw new IllegalStateException("Missing current AT bytes: "+address);
                        digest.update(address.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                        digest.update(java.nio.ByteBuffer.allocate(4).putInt(bytes.length).array());
                        digest.update(bytes);
                    }
                }
                continue;
            } else if (table.equals("ATBlockResults"))
                // The replay range has no blobs before its first execution when qualifying
                // a migrated v53 database. Compare stable history below that range.
                query="SELECT * FROM ATBlockResults WHERE block_height<"+firstHeight+" ORDER BY block_height";
            else
                query="SELECT * FROM "+table+" ORDER BY 1,2";
            try(java.sql.Statement statement=connection.createStatement();java.sql.ResultSet rs=statement.executeQuery(query)) {
                while(rs.next()) for(int column=1;column<=rs.getMetaData().getColumnCount();++column) {
                    // Internal payload IDs and the format marker are storage details, not ledger state.
                    String name=rs.getMetaData().getColumnName(column);
					boolean parentPointer = table.equals("ATCurrentState") && (name.equalsIgnoreCase("previous_state_height")
							|| name.equalsIgnoreCase("previous_payload_hash"));
					Object value=table.equals("ATStoreInfo") && name.equalsIgnoreCase("format_version")
							? Integer.valueOf(1) : parentPointer ? null : rs.getObject(column);
                    byte[] bytes=value==null?new byte[]{0}:value instanceof byte[]?(byte[])value:value.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
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
        if (args.length != 3) throw new IllegalArgumentException("ATReplay <marked-work-directory> <count> <prepare|replay|migrate>");
        Path work = Paths.get(args[0]).toRealPath();
        if (!Files.isRegularFile(work.resolve("DISPOSABLE_AT_REPLAY"))) throw new IllegalArgumentException("Missing disposable marker");
        Path db = work.resolve("db").toRealPath();
        if (!db.startsWith(work) || db.equals(Paths.get("/home/qortal/qortal/db").toAbsolutePath())) throw new IllegalArgumentException("Unsafe database path");
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
        Security.addProvider(new org.bouncycastle.jsse.provider.BouncyCastleJsseProvider());
        Settings.fileInstance(work.resolve("settings.json").toString());
        if (!Paths.get(Settings.getInstance().getRepositoryPath()).toRealPath().equals(db)) throw new IllegalArgumentException("Settings repository differs from disposable DB");
        NTP.setFixedOffset(0L);
        RepositoryFactory factory = new HSQLDBRepositoryFactory(Controller.getRepositoryUrl());
        RepositoryManager.setRepositoryFactory(factory);
        int count = Integer.parseInt(args[1]);
        if (count < 0 || count > 10000) throw new IllegalArgumentException("count must be 0..10000");
        Path blocks = work.resolve("blocks");
        Files.createDirectories(blocks);
        java.util.concurrent.locks.ReentrantLock lock = Controller.getInstance().getBlockchainLock();
        lock.lock();
        try (Repository r = factory.getRepository()) {
            if (args[2].equals("inspect")) {
                java.lang.reflect.Field field=Class.forName("org.qortal.repository.hsqldb.HSQLDBRepository").getDeclaredField("connection");
                field.setAccessible(true);
                java.sql.Connection c=(java.sql.Connection)field.get(r);
                try (java.sql.Statement statement=c.createStatement()) {
                    for (String sql:new String[]{
                        "SELECT MIN(block_height),MAX(block_height),COUNT(*) FROM ATBlockResults",
                        "SELECT MIN(block_height),MAX(block_height),COUNT(*) FROM ATBlockResults WHERE block_height>(SELECT MAX(block_height)-10000 FROM ATBlockResults)",
                        "SELECT MIN(height),MAX(height),COUNT(*) FROM Blocks"}) {
                        try (java.sql.ResultSet rs=statement.executeQuery(sql)) { while(rs.next()) { StringBuilder line=new StringBuilder(sql+" => "); for(int i=1;i<=rs.getMetaData().getColumnCount();++i)line.append(rs.getObject(i)).append(' ');System.out.println(line); } }
                    }
                }
				Path scope=work.resolve("fingerprint-first-height.txt");
				Path range=work.resolve("range.txt");
				if (Files.isRegularFile(scope) || Files.isRegularFile(range)) {
					int first=Files.isRegularFile(scope) ? Integer.parseInt(Files.readString(scope).trim())
							: Integer.parseInt(Files.readString(range).trim().split(" ")[0]);
					System.out.println("LEDGER_FINGERPRINT " + ledgerFingerprint(r,first));
				}
            } else if (args[2].equals("seal")) {
                // A pruned parent can stop rewind before the exported lower bound. Preserve the
                // original fingerprint scope while sealing the successfully committed suffix.
                String[] range=Files.readString(work.resolve("range.txt")).trim().split(" ");
                int originalFirst=Integer.parseInt(range[0]),last=Integer.parseInt(range[1]);
                int first=r.getBlockRepository().getBlockchainHeight()+1;
                if(first<originalFirst || first>last)throw new IllegalStateException("No valid rewound suffix to seal");
                if(!Files.isRegularFile(work.resolve("ledger-before.txt")))throw new IllegalStateException("Missing original fingerprint");
                Path scope=work.resolve("fingerprint-first-height.txt");
                if(!Files.exists(scope))Files.writeString(scope,String.valueOf(originalFirst));
                Files.writeString(work.resolve("range.txt"),first+" "+last+"\n");
                System.out.println("SEALED_REPLAY first="+first+" last="+last+" blocks="+(last-first+1));
            } else if (args[2].equals("prepare")) {
                int tip = r.getBlockRepository().getBlockchainHeight();
                // Export the entire selected range before the first destructive operation.
                for (int height = tip-count+1; height <= tip; ++height) {
                    BlockData data = r.getBlockRepository().fromHeight(height);
                    if (data == null) throw new IllegalStateException("Missing block " + height);
                    Block source=new Block(r,data);
                    if(source.getATStates().stream().filter(state -> !state.isInitial()).count()!=data.getATCount())
                        throw new IllegalStateException("Incomplete retained receipts at " + height);
                    Files.write(blocks.resolve(height+".bin"),BlockTransformer.toBytes(source));
                    if (height % 100 == 0) System.out.println("EXPORT " + height);
                }
                Files.writeString(work.resolve("range.txt"),(tip-count+1)+" "+tip+"\n");
                Files.writeString(work.resolve("ledger-before.txt"),ledgerFingerprint(r,tip-count+1));
                System.out.println("Original ledger fingerprint recorded");
                r.discardChanges();
                for (int i=0; i<count; ++i) {
                    Block block = new Block(r,r.getBlockRepository().getLastBlock());
                    int height=block.getBlockData().getHeight();
                    block.orphan(); r.saveChanges();
                    if (i % 100 == 0) System.out.println("ORPHAN " + height);
                }
            } else if (args[2].equals("rewind")) {
                String[] range=Files.readString(work.resolve("range.txt")).trim().split(" ");
                int first=Integer.parseInt(range[0]),last=Integer.parseInt(range[1]);
                if (!Files.isRegularFile(work.resolve("ledger-before.txt"))) throw new IllegalStateException("Missing original ledger fingerprint");
                int height=r.getBlockRepository().getBlockchainHeight();
                if (height<first-1 || height>last) throw new IllegalStateException("Rewind outside exported range");
                while (height>=first) {
                    if (!Files.isRegularFile(blocks.resolve(height+".bin"))) throw new IllegalStateException("Missing exported block "+height);
                    new Block(r,r.getBlockRepository().getLastBlock()).orphan();r.saveChanges();
                    if (height%100==0)System.out.println("ORPHAN "+height);
                    --height;
                }
            } else if (args[2].equals("replay") || args[2].equals("benchmark")) {
                String[] range=Files.readString(work.resolve("range.txt")).trim().split(" ");
                int first=Integer.parseInt(range[0]), last=Integer.parseInt(range[1]);
                boolean partial=args[2].equals("benchmark");
                if (partial) {
                    if(count<1)throw new IllegalArgumentException("Benchmark count must be positive");
                    last=Math.min(last,first+count-1);
                    System.out.println("BENCHMARK_RANGE first="+first+" last="+last+" (full block validation; no original-tip ledger comparison)");
                }
                if (r.getBlockRepository().getBlockchainHeight()!=first-1) throw new IllegalStateException("Replay parent mismatch");
                for (int height=first; height<=last; ++height) {
                    BlockTransformation t=BlockTransformer.fromBytes(Files.readAllBytes(blocks.resolve(height+".bin")));
                    // Height is supplied by synchronization, not serialized in network block bytes.
                    t.getBlockData().setHeight(height);
                    Block block=t.getAtStatesHash()==null ? new Block(r,t.getBlockData(),t.getTransactions(),t.getAtStates())
                            : new Block(r,t.getBlockData(),t.getTransactions(),t.getAtStatesHash());
                    Path qualifiedHash=work.resolve("expected-at-hashes").resolve(height+".bin");
                    if(Files.isRegularFile(qualifiedHash)) {
                        java.lang.reflect.Field hashField=Block.class.getDeclaredField("atStatesHash");
                        hashField.setAccessible(true);hashField.set(block,Files.readAllBytes(qualifiedHash));
                    }
                    if(!block.isSignatureValid())throw new IllegalStateException("Invalid block signature at "+height);
                    for(Transaction tx:block.getTransactions())tx.setInitialApprovalStatus();
                    block.preProcess();
                    Block.ValidationResult result=block.isValid();
                    if (result!=Block.ValidationResult.OK) {
                        diagnoseAtMismatch(block,work);
                        throw new IllegalStateException("Block " + height + " failed: " + result);
                    }
                    for (Transaction tx:block.getTransactions()) if (tx.getTransactionData().getType()!=Transaction.TransactionType.AT)
                        r.getTransactionRepository().save(tx.getTransactionData());
                    block.process(); r.saveChanges();
                    if (!Arrays.equals(t.getBlockData().getSignature(),r.getBlockRepository().getLastBlock().getSignature()))
                        throw new IllegalStateException("Tip mismatch");
                    if (height % 100 == 0) System.out.println("REPLAY " + height);
                }
                Path scope=work.resolve("fingerprint-first-height.txt");
                int fingerprintFirst=Files.exists(scope)?Integer.parseInt(Files.readString(scope).trim()):first;
                if(!partial && !Files.readString(work.resolve("ledger-before.txt")).equals(ledgerFingerprint(r,fingerprintFirst)))
                    throw new IllegalStateException("Replayed ledger differs from original: accounts, balances, definitions, heads, selected history, recipients or chain marker");
                System.out.println(partial ? "BENCHMARK_COMPLETE blocks="+(last-first+1) : "LEDGER_MATCH");
            } else if (!args[2].equals("migrate")) throw new IllegalArgumentException("Unknown operation");
            r.discardChanges();
            System.out.println("COMPLETE " + args[2] + " height=" + r.getBlockRepository().getBlockchainHeight());
        } finally { lock.unlock(); RepositoryManager.closeRepositoryFactory(); NTP.shutdownNow(); }
        System.exit(0);
    }

    @SuppressWarnings("unchecked")
    private static void diagnoseAtMismatch(Block block,Path work) {
        try {
            java.lang.reflect.Field statesField=Block.class.getDeclaredField("ourAtStates");
            java.lang.reflect.Field feesField=Block.class.getDeclaredField("ourAtFees");
            statesField.setAccessible(true);feesField.setAccessible(true);
            List<org.qortal.data.at.ATStateData> states=(List<org.qortal.data.at.ATStateData>)statesField.get(block);
            long fees=feesField.getLong(block);
            System.err.println("AT_DIAGNOSTIC expected_count="+block.getBlockData().getATCount()+" actual_count="+(states==null?-1:states.size())
                    +" expected_fees="+block.getBlockData().getATFees()+" actual_fees="+fees);
            StringBuilder dump=new StringBuilder();
            if(states!=null)for(int i=0;i<states.size();++i){org.qortal.data.at.ATStateData s=states.get(i);String line=s.getATAddress()+" "+org.qortal.utils.Base58.encode(s.getStateHash())+" "+s.getFees();if(i<5)System.err.println("AT_DIAGNOSTIC_STATE "+line);dump.append(line).append('\n');}
            Files.writeString(work.resolve("at-mismatch-actual.txt"),dump.toString());
        } catch(Exception e) { System.err.println("AT_DIAGNOSTIC unavailable: "+e); }
    }
}
