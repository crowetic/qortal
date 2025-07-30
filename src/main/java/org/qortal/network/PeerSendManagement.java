package org.qortal.network;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PeerSendManagement {

    private static final Logger LOGGER = LogManager.getLogger(PeerSendManagement.class);

    private final Map<String, PeerSendManager> peerSendManagers = new ConcurrentHashMap<>();

    public PeerSendManager getOrCreateSendManager(Peer peer) {
        return peerSendManagers.computeIfAbsent(peer.toString(), p -> new PeerSendManager(peer));
    }

    private PeerSendManagement() {

        ScheduledExecutorService cleaner = Executors.newSingleThreadScheduledExecutor();

        cleaner.scheduleAtFixedRate(() -> {
            long idleCutoff = TimeUnit.MINUTES.toMillis(2);
            Iterator<Map.Entry<String, PeerSendManager>> iterator = peerSendManagers.entrySet().iterator();

            while (iterator.hasNext()) {
                Map.Entry<String, PeerSendManager> entry = iterator.next();

                PeerSendManager manager = entry.getValue();

                if (manager.isIdle(idleCutoff)) {
                    iterator.remove(); // SAFE removal during iteration
                    manager.shutdown();
                    LOGGER.debug("Cleaned up PeerSendManager for peer {}", entry.getKey());
                }
            }
        }, 0, 5, TimeUnit.MINUTES);
    }

    private static PeerSendManagement instance;

    public static PeerSendManagement getInstance() {

        if( instance == null ) {
            instance = new PeerSendManagement();
        }

        return instance;
    }
}
