import java.nio.file.*;
import java.util.*;
import org.qortal.block.Block;
import org.qortal.controller.Controller;
import org.qortal.data.account.AccountBalanceData;
import org.qortal.repository.*;
import org.qortal.repository.hsqldb.HSQLDBRepositoryFactory;
import org.qortal.test.common.*;
import org.qortal.utils.NTP;

/** Supplemental synthetic file-backed soak; never a substitute for historical replay. */
public final class ATSoak {
    public static void main(String[] args) {
        try { run(args); } catch(Throwable failure) { failure.printStackTrace();System.exit(1); }
    }
    private static void run(String[] args) throws Exception {
        Path directory=Paths.get(args[0]).toRealPath();
        boolean resume=args.length>2 && args[2].equals("resume");
        if(!Files.isRegularFile(directory.resolve("DISPOSABLE_AT_REPLAY")) || (!resume && Files.exists(directory.resolve("blockchain.properties"))))
            throw new IllegalArgumentException("Requires a marked empty disposable DB directory");
        int count=Integer.parseInt(args[1]);
        Class.forName(Common.class.getName());NTP.setFixedOffset(0L);
        RepositoryManager.setRepositoryFactory(new HSQLDBRepositoryFactory("jdbc:hsqldb:file:"+directory.resolve("blockchain")));
        if(!resume)Common.resetBlockchain();
        try(Repository r=RepositoryManager.getRepository()) {
            if(!resume) {
            List<AccountBalanceData> freeze=new ArrayList<>();
            for(int i=0;i<168;++i) {
                String address=AtUtils.doDeployAT(r,Common.getTestAccount(r,"alice"),AtUtils.buildSimpleAT(),1_00000000L).getATAccount().getAddress();
                freeze.add(new AccountBalanceData(address,0,0));
            }
            r.getAccountRepository().setAssetBalances(freeze);r.saveChanges();BlockUtils.mintBlock(r);
            }
            long frozen=r.getATRepository().getATsForExecution(r.getBlockRepository().getBlockchainHeight()+1).stream().filter(at -> at.getIsFrozen()).count();
            if(frozen!=168)throw new AssertionError("Expected 168 frozen contracts, got "+frozen);
            System.out.println("SOAK_BEGIN blocks="+count+" frozen_contracts=168");
            for(int i=0;i<count;++i) {
                BlockUtils.mintBlock(r);
                if(i%100==0)System.out.println("SOAK_PROGRESS "+i);
            }
            System.out.println("SOAK_COMPLETE height="+r.getBlockRepository().getBlockchainHeight());
            r.discardChanges();
        } finally { RepositoryManager.closeRepositoryFactory(); }
        System.exit(0);
    }
}
