package org.qortal.controller;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.Test;
import org.qortal.controller.arbitrary.PeerMessage;
import org.qortal.data.block.BlockSummaryData;
import org.qortal.data.network.PeerData;
import org.qortal.network.Peer;
import org.qortal.network.PeerAddress;
import org.qortal.network.message.BlockSummariesV2Message;
import org.qortal.network.message.Message;
import org.qortal.test.common.Common;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class BlockSummaryBatchTests extends Common {
    @Test
    @SuppressWarnings("unchecked")
    public void lateReplyDoesNotDiscardFollowingPeerTipAndSyncRequest() throws Exception {
        Common.useDefaultSettings();
        Peer peer = new Peer(new PeerData(PeerAddress.fromString("198.51.100.4:12392")), Peer.NETWORK);
        BlockSummaryData tip = new BlockSummaryData(99, new byte[128], new byte[32], 0, 1234L, 0, new byte[128]);
        Message late = Message.fromByteBuffer(ByteBuffer.wrap(new BlockSummariesV2Message(Collections.singletonList(tip)).toBytes()));
        late.setId(7);
        Message current = Message.fromByteBuffer(ByteBuffer.wrap(new BlockSummariesV2Message(Collections.singletonList(tip)).toBytes()));
        List<PeerMessage> queue = (List<PeerMessage>) FieldUtils.readStaticField(Controller.class, "SIGNATURE_MESSAGE_LIST", true);
        boolean requested = Synchronizer.getInstance().isSyncRequested();
        try {
            queue.addAll(Arrays.asList(new PeerMessage(peer, late), new PeerMessage(peer, current)));
            FieldUtils.writeStaticField(Synchronizer.class, "requestSync", false, true);
            Method process = Controller.class.getDeclaredMethod("processNetworkBlockSummariesV2Messages");
            process.setAccessible(true);
            process.invoke(null);
            assertEquals(99, peer.getChainTipData().getHeight());
            assertTrue(Synchronizer.getInstance().isSyncRequested());
            assertTrue(queue.isEmpty());
        } finally {
            queue.clear();
            FieldUtils.writeStaticField(Synchronizer.class, "requestSync", requested, true);
        }
    }
}
