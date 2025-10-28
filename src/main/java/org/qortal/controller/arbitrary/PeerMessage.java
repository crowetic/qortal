package org.qortal.controller.arbitrary;

import org.qortal.controller.Controller;
import org.qortal.network.Peer;
import org.qortal.network.message.Message;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;

public class PeerMessage {
    Peer peer;
    Message message;
    //private static final Logger LOGGER = LogManager.getLogger(PeerMessage.class);

    public PeerMessage(Peer peer, Message message) {
        this.peer = (Peer) peer;
        this.message = message;
    }

    public Peer getPeer() {
        //try {
        //    if (Class.forName("org.qortal.network.Peer").isInstance(peer)) {
        //        return (Peer) peer;
        //    } else {
        //        return (ReticulumPeer) peer;
        //    }
        //}  catch (ClassNotFoundException e) {
        //    LOGGER.error("Could not find peer class", e);
        //}
        return peer;
    }

    public Message getMessage() {
        return message;
    }
}
