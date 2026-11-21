package org.qortal.controller;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.Before;
import org.junit.Test;
import org.qortal.block.Block;
import org.qortal.data.block.BlockData;
import org.qortal.data.block.BlockSummaryData;
import org.qortal.data.network.PeerData;
import org.qortal.network.Peer;
import org.qortal.network.PeerAddress;
import org.qortal.network.message.BlockV2Message;
import org.qortal.network.message.BlockSummariesV2Message;
import org.qortal.network.message.BlocksMessage;
import org.qortal.network.message.Message;
import org.qortal.network.message.MessageType;
import org.qortal.network.message.SignaturesMessage;
import org.qortal.repository.DataException;
import org.qortal.repository.Repository;
import org.qortal.repository.RepositoryManager;
import org.qortal.test.common.BlockUtils;
import org.qortal.test.common.Common;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class SynchronizerArchiveFallbackTests extends Common {

    @Before
    public void beforeTest() throws DataException {
        Common.useDefaultSettings();
    }

    @Test
    public void testEmptyFastBatchFallsBackToIndividualBlock() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            BlockUtils.mintBlock(repository);
            BlockData commonBlock = repository.getBlockRepository().fromHeight(1);
            BlockData nextBlockData = repository.getBlockRepository().fromHeight(2);

            BlockV2Message outgoingBlock = new BlockV2Message(new Block(repository, nextBlockData));
            byte[] blockPayload = (byte[]) FieldUtils.readField(outgoingBlock, "dataBytes", true);
            Message incomingBlock = BlockV2Message.fromByteBuffer(1, ByteBuffer.wrap(blockPayload));
            TestPeer peer = new TestPeer(incomingBlock);

            Method fetchMethod = Synchronizer.class.getDeclaredMethod("fetchAppendPlan", Repository.class,
                    Peer.class, BlockData.class, int.class, int.class, List.class, boolean.class);
            fetchMethod.setAccessible(true);
            Object plan = fetchMethod.invoke(Synchronizer.getInstance(), repository, peer, commonBlock,
                    1, 2, Collections.singletonList(new BlockSummaryData(nextBlockData)), true);

            assertEquals(Synchronizer.SynchronizationResult.OK, FieldUtils.readField(plan, "result", true));
            assertEquals(true, FieldUtils.readField(plan, "fastBatchUnavailable", true));
            List<?> blocks = (List<?>) FieldUtils.readField(plan, "blocks", true);
            assertEquals(1, blocks.size());
            assertEquals(1, peer.fastBatchRequests);
            assertEquals(1, peer.individualBlockRequests);
        }
    }

    @Test
    public void testEmptyArchivedSummariesFallBackToSuccessorSignature() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            BlockUtils.mintBlock(repository);
            BlockData genesis = repository.getBlockRepository().fromHeight(1);
            BlockData nextBlock = repository.getBlockRepository().fromHeight(2);
            SummaryFallbackPeer peer = new SummaryFallbackPeer(nextBlock);
            List<BlockSummaryData> summaries = new java.util.ArrayList<>();

            Synchronizer.SynchronizationResult result = Synchronizer.getInstance().fetchSummariesFromCommonBlock(
                    repository, peer, 1, true, summaries, true);

            assertEquals(Synchronizer.SynchronizationResult.OK, result);
            assertEquals(1, summaries.size());
            assertArrayEquals(genesis.getSignature(), summaries.get(0).getSignature());
            assertEquals(1, peer.summaryRequests);
            assertEquals(1, peer.signatureRequests);
        }
    }

    private static final class TestPeer extends Peer {
        private final Message individualBlockResponse;
        private int fastBatchRequests;
        private int individualBlockRequests;

        private TestPeer(Message individualBlockResponse) {
            super(new PeerData(new PeerAddress("127.0.0.1:12392")), Peer.NETWORK);
            this.individualBlockResponse = individualBlockResponse;
            this.setPeersVersion("qortal-6.1.9", 0x0600010009L);
        }

        @Override
        public Message getResponseWithTimeout(Message message, int timeout) {
            if (message.getType() == MessageType.GET_BLOCKS) {
                ++this.fastBatchRequests;
                return new BlocksMessage(Collections.emptyList());
            }

            if (message.getType() == MessageType.GET_BLOCK) {
                ++this.individualBlockRequests;
                return this.individualBlockResponse;
            }

            fail("Unexpected peer request: " + message.getType());
            return null;
        }
    }

    private static final class SummaryFallbackPeer extends Peer {
        private final BlockData nextBlock;
        private final Message signaturesResponse;
        private int summaryRequests;
        private int signatureRequests;

        private SummaryFallbackPeer(BlockData nextBlock) throws IllegalAccessException {
            super(new PeerData(new PeerAddress("127.0.0.1:12392")), Peer.NETWORK);
            this.nextBlock = nextBlock;
            this.setPeersVersion("qortal-6.1.9", 0x0600010009L);
            this.setChainTipData(new BlockSummaryData(nextBlock));
            SignaturesMessage outgoing = new SignaturesMessage(Collections.singletonList(nextBlock.getSignature()));
            byte[] payload = (byte[]) FieldUtils.readField(outgoing, "dataBytes", true);
            this.signaturesResponse = SignaturesMessage.fromByteBuffer(1, ByteBuffer.wrap(payload));
        }

        @Override
        public Message getResponseWithTimeout(Message message, int timeout) {
            if (message.getType() == MessageType.GET_BLOCK_SUMMARIES) {
                ++this.summaryRequests;
                return BlockSummariesV2Message.fromByteBuffer(1, ByteBuffer.allocate(0));
            }

            if (message.getType() == MessageType.GET_SIGNATURES_V2) {
                ++this.signatureRequests;
                return this.signaturesResponse;
            }

            fail("Unexpected peer request: " + message.getType());
            return null;
        }
    }
}
