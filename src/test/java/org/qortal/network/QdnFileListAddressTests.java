package org.qortal.network;

import org.junit.Before;
import org.junit.Test;
import org.qortal.data.network.PeerData;
import org.qortal.network.helper.PeerCapabilities;
import org.qortal.network.message.ArbitraryDataFileListMessage;
import org.qortal.network.message.GetArbitraryDataFileListMessage;
import org.qortal.network.message.HelloV2Message;
import org.qortal.network.message.Message;
import org.qortal.test.common.Common;

import java.nio.ByteBuffer;
import java.util.Collections;

import static org.junit.Assert.*;

public class QdnFileListAddressTests extends Common {
    @Before
    public void setUp() throws Exception {
        Common.useDefaultSettings();
    }

    private Message roundTrip(Message message) throws Exception {
        return Message.fromByteBuffer(ByteBuffer.wrap(message.toBytes()).asReadOnlyBuffer());
    }

    @Test
    public void missingContactAddressIsOptionalForRequestsAndNegotiatedForReplies() throws Exception {
        Peer peer = new Peer(new PeerData(PeerAddress.fromString("198.51.100.5:12394")), Peer.NETWORKDATA);
        assertTrue(peer.canReceiveQdnFileList("198.51.100.1:12394"));
        assertFalse(peer.canReceiveQdnFileList(null));

        HelloV2Message hello = new HelloV2Message(1L, "qortal-6.1.9", "198.51.100.1:12394",
                Collections.singletonMap(PeerCapabilities.QDN_OPTIONAL_SOURCE_ADDRESS, true), Peer.NETWORKDATA);
        peer.setPeersCapabilities(((HelloV2Message) roundTrip(hello)).getCapabilities());
        assertTrue(peer.canReceiveQdnFileList(null));

        byte[] signature = new byte[64];
        GetArbitraryDataFileListMessage request = new GetArbitraryDataFileListMessage(signature, null, 1L, 0, null);
        assertNull(((GetArbitraryDataFileListMessage) roundTrip(request)).getRequestingPeer());
        ArbitraryDataFileListMessage reply = new ArbitraryDataFileListMessage(signature,
                Collections.singletonList(new byte[32]), 1L, 0, null, "node", true, false);
        ArbitraryDataFileListMessage decoded = (ArbitraryDataFileListMessage) roundTrip(reply);
        assertNull(decoded.getPeerAddress());
        assertFalse(decoded.isDirectConnectable());
        assertTrue(decoded.isRelayPossible());
        assertEquals("node", decoded.getNodeId());
    }
}
