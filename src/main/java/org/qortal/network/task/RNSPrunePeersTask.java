package org.qortal.network.task;

import org.qortal.controller.Controller;
//import org.qortal.network.RNSNetwork;
//import org.qortal.repository.DataException;
import org.qortal.utils.ExecuteProduceConsume.Task;

public class RNSPrunePeersTask implements Task {
    public RNSPrunePeersTask() {
    }

    @Override
    public String getName() {
        return "PrunePeersTask";
    }

    @Override
    public void perform() throws InterruptedException {
        Controller.getInstance().doRNSPrunePeers();
        //try {
        //    log.debug("Pruning peers...");
        //    RNSNetwork.getInstance().prunePeers();
        //} catch (DataException e) {
        //    log.warn(String.format("Repository issue when trying to prune peers: %s", e.getMessage()));
        //}
    }
}
