package org.qortal.controller;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.qortal.account.PrivateKeyAccount;
import org.qortal.block.Block;
import org.qortal.block.BlockChain;
import org.qortal.data.block.BlockData;
import org.qortal.data.block.BlockSummaryData;
import org.qortal.data.network.PeerData;
import org.qortal.data.transaction.PaymentTransactionData;
import org.qortal.network.Peer;
import org.qortal.network.PeerAddress;
import org.qortal.network.message.BlockSummariesV2Message;
import org.qortal.network.message.BlockV2Message;
import org.qortal.network.message.GetBlocksMessage;
import org.qortal.network.message.GetSignaturesV2Message;
import org.qortal.network.message.SignaturesMessage;
import org.qortal.network.message.BlocksMessage;
import org.qortal.network.message.Message;
import org.qortal.network.message.MessageType;
import org.qortal.repository.DataException;
import org.qortal.repository.Repository;
import org.qortal.repository.RepositoryManager;
import org.qortal.settings.Settings;
import org.qortal.test.common.AtUtils;
import org.qortal.test.common.BlockUtils;
import org.qortal.test.common.Common;
import org.qortal.test.common.TransactionUtils;
import org.qortal.test.common.transaction.TestTransaction;
import org.qortal.utils.NTP;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;
import static org.qortal.controller.Synchronizer.SynchronizationResult.*;

public class SynchronizerPartialProgressTests extends Common {
    @Before
    public void beforeTest() throws DataException {
        Common.useDefaultSettings();
    }

    @After
    public void restoreClock() {
        NTP.setFixedOffset(Settings.getInstance().getTestNtpOffset());
    }

    @Test
    public void blockTimeoutCommitsPrefixAndNextPeerContinuesFromIt() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            Fixture fixture = new Fixture(repository);
            ScriptedPeer peer = new ScriptedPeer(fixture, 2);

            assertEquals(NO_REPLY, append(repository, peer, fixture.base, fixture.summaries));
            assertEquals(1, peer.batchRequests);
            assertEquals(3, peer.blockRequests);
            assertCommittedTip(repository, fixture.blocks.get(1));
            assertArrayEquals(fixture.stateHashes.get(1),
                    repository.getATRepository().getLatestATState(fixture.atAddress).getStateHash());

            // A fresh session starts after the committed prefix, not after the original tip.
            ScriptedPeer nextPeer = new ScriptedPeer(fixture, 3);
            assertEquals(OK, append(repository, nextPeer, fixture.blocks.get(1), fixture.summaries.subList(2, 3)));
            assertEquals(1, nextPeer.blockRequests);
            assertCommittedTip(repository, fixture.blocks.get(2));
            assertArrayEquals(fixture.stateHashes.get(2),
                    repository.getATRepository().getLatestATState(fixture.atAddress).getStateHash());
            repository.discardChanges();
            Common.orphanCheck();
        }
    }

    @Test
    public void signatureTimeoutAlsoCommitsPrefix() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            Fixture fixture = new Fixture(repository);
            ScriptedPeer peer = new ScriptedPeer(fixture, 3);
            assertEquals(NO_REPLY, append(repository, peer, fixture.base, fixture.summaries.subList(0, 1)));
            assertEquals(1, peer.blockRequests);
            assertEquals(1, peer.signatureRequests);
            assertCommittedTip(repository, fixture.blocks.get(0));
        }
    }

    @Test
    public void individualOnlyModeAlsoCommitsPrefix() throws Exception {
        FieldUtils.writeField(Settings.getInstance(), "fastSyncEnabled", false, true);
        try (Repository repository = RepositoryManager.getRepository()) {
            Fixture fixture = new Fixture(repository);
            ScriptedPeer peer = new ScriptedPeer(fixture, 2);
            assertEquals(NO_REPLY, append(repository, peer, fixture.base, fixture.summaries));
            assertEquals(0, peer.batchRequests);
            assertCommittedTip(repository, fixture.blocks.get(1));
        }
    }

    @Test
    public void timeoutBeforeAnyBlockLeavesTipUnchanged() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            Fixture fixture = new Fixture(repository);
            ScriptedPeer peer = new ScriptedPeer(fixture, 0);
            assertEquals(NO_REPLY, append(repository, peer, fixture.base, fixture.summaries));
            assertCommittedTip(repository, fixture.base);
        }
    }

    @Test
    public void wrongBlockResponseDoesNotApplyPrefix() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            Fixture fixture = new Fixture(repository);
            ScriptedPeer peer = new ScriptedPeer(fixture, 3);
            peer.wrongSecondBlock = true;
            assertEquals(INVALID_DATA, append(repository, peer, fixture.base, fixture.summaries));
            assertCommittedTip(repository, fixture.base);
        }
    }

    @Test
    public void noncontiguousSignedBlocksDoNotApplyPrefix() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            Fixture fixture = new Fixture(repository);
            ScriptedPeer peer = new ScriptedPeer(fixture, 3);
            assertEquals(INVALID_DATA, append(repository, peer, fixture.base,
                    Arrays.asList(fixture.summaries.get(0), fixture.summaries.get(2))));
            assertCommittedTip(repository, fixture.base);
        }
    }

    @Test
    public void stalePeerTipDoesNotApplyPartialPrefix() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            Fixture fixture = new Fixture(repository);
            ScriptedPeer peer = new ScriptedPeer(fixture, 1);
            BlockData lastBlock = fixture.blocks.get(2);
            assertNotNull(Controller.getMinimumLatestBlockTimestamp());
            peer.setChainTipData(new BlockSummaryData(lastBlock.getHeight(), lastBlock.getSignature(),
                    lastBlock.getMinterPublicKey(), 1L));
            assertEquals(CHAIN_TIP_TOO_OLD, append(repository, peer, fixture.base, fixture.summaries));
            assertCommittedTip(repository, fixture.base);
        }
    }

    @Test
    public void tipChangeWhileFetchingPreventsApplyingPartialPrefix() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            Fixture fixture = new Fixture(repository);
            ScriptedPeer peer = new ScriptedPeer(fixture, 1);
            peer.onMissingBlock = () -> {
                try {
                    BlockUtils.mintBlock(repository);
                } catch (DataException e) {
                    throw new AssertionError(e);
                }
            };
            assertEquals(NOTHING_TO_DO, append(repository, peer, fixture.base, fixture.summaries));
            assertEquals(fixture.base.getHeight() + 1, repository.getBlockRepository().getBlockchainHeight());
            repository.discardChanges();
        }
    }

    @Test
    public void reorgFetchStillDiscardsIncompleteReplacement() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            Fixture fixture = new Fixture(repository);
            ScriptedPeer peer = new ScriptedPeer(fixture, 1);
            Method method = Synchronizer.class.getDeclaredMethod("fetchReorgPlan", Repository.class,
                    Peer.class, BlockData.class, int.class, List.class);
            Object plan = invoke(method, repository, peer, fixture.base,
                    fixture.blocks.get(2).getHeight(), fixture.summaries);
            assertEquals(NO_REPLY, FieldUtils.readField(plan, "result", true));
            assertTrue(((List<?>) FieldUtils.readField(plan, "blocks", true)).isEmpty());
            assertCommittedTip(repository, fixture.base);
        }
    }

    @Test
    public void reorgDownloads240BlocksInBatchesAndCommitsSelectedTip() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            Fixture fixture = new Fixture(repository, 240);
            forkLocally(repository, fixture);
            ScriptedPeer legacy = new ScriptedPeer(fixture, 240);
            legacy.useLegacyVersion();
            assertEquals(OK, fetchReorg(repository, fixture, legacy));
            assertEquals(240, legacy.blockRequests);
            assertEquals(0, legacy.batchRequests);
            ReorgPeer peer = new ReorgPeer(fixture);
            assertEquals(OK, Synchronizer.getInstance().synchronize(peer, false));
            assertEquals(0, peer.blockRequests);
            // Seven initial successors, then 100 + 100 from the next signature reply, then 33.
            assertEquals(4, peer.batchRequests);
            assertCommittedTip(repository, fixture.blocks.get(239));
            assertArrayEquals(fixture.stateHashes.get(239),
                    repository.getATRepository().getLatestATState(fixture.atAddress).getStateHash());
        }
    }

    @Test
    public void forkBatchSettingCanBeDisabled() throws Exception {
        FieldUtils.writeField(Settings.getInstance(), "fastSyncEnabledWhenResolvingFork", false, true);
        try (Repository repository = RepositoryManager.getRepository()) {
            Fixture fixture = new Fixture(repository);
            ScriptedPeer peer = new ScriptedPeer(fixture, 3);
            assertEquals(OK, fetchReorg(repository, fixture, peer));
            assertEquals(0, peer.batchRequests);
            assertEquals(3, peer.blockRequests);
        }
    }

    @Test
    public void reorgContinuesFromShortBatchWhenPeerFallsBackToIndividualBlocks() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            Fixture fixture = new Fixture(repository);
            forkLocally(repository, fixture);
            ReorgPeer peer = new ReorgPeer(fixture);
            peer.batchReplySizes = new int[]{1, 0};
            assertEquals(OK, Synchronizer.getInstance().synchronize(peer, false));
            assertEquals(2, peer.batchRequests);
            assertEquals(2, peer.blockRequests);
            assertCommittedTip(repository, fixture.blocks.get(2));
            assertArrayEquals(fixture.stateHashes.get(2),
                    repository.getATRepository().getLatestATState(fixture.atAddress).getStateHash());
        }
    }

    @Test
    public void reorgRejectsSummaryWeightThatDisagreesWithBlock() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            Fixture fixture = new Fixture(repository);
            BlockData localFork = forkLocally(repository, fixture);
            ReorgPeer peer = new ReorgPeer(fixture);
            BlockData block = fixture.blocks.get(1);
            peer.summaries.set(2, new BlockSummaryData(block.getHeight(), block.getSignature(), block.getMinterPublicKey(),
                    block.getOnlineAccountsCount() + 1, block.getTimestamp(), block.getTransactionCount(), block.getReference()));
            assertEquals(INVALID_DATA, Synchronizer.getInstance().synchronize(peer, false));
            assertCommittedTip(repository, localFork);
        }
    }

    @Test
    public void reorgRejectsBatchThatDiffersFromSelectedSummaries() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            Fixture fixture = new Fixture(repository);
            BlockData localFork = forkLocally(repository, fixture);
            ReorgPeer peer = new ReorgPeer(fixture);
            // Advertise the local sibling's signature at the second replacement height.
            // The returned bodies still form a valid chain, but differ from the selected signatures.
            peer.summaries.set(2, new BlockSummaryData(localFork));
            assertEquals(INVALID_DATA, Synchronizer.getInstance().synchronize(peer, false));
            assertCommittedTip(repository, localFork);
        }
    }

    @Test
    public void reorgRechecksTipAfterBatchDownload() throws Exception {
        try (Repository repository = RepositoryManager.getRepository()) {
            Fixture fixture = new Fixture(repository);
            forkLocally(repository, fixture);
            ReorgPeer peer = new ReorgPeer(fixture);
            BlockData[] changed = new BlockData[1];
            peer.onBatch = () -> {
                BlockUtils.mintBlock(repository);
                changed[0] = repository.getBlockRepository().getLastBlock();
            };
            assertEquals(NOTHING_TO_DO, Synchronizer.getInstance().synchronize(peer, false));
            assertCommittedTip(repository, changed[0]);
        }
    }

    private static BlockData forkLocally(Repository repository, Fixture fixture) throws DataException {
        NTP.setFixedOffset(Settings.getInstance().getTestNtpOffset());
        PrivateKeyAccount alice = Common.getTestAccount(repository, "alice");
        PaymentTransactionData payment = new PaymentTransactionData(TestTransaction.generateBase(alice),
                Common.getTestAccount(repository, "bob").getAddress(), 123L);
        TransactionUtils.signAndMint(repository, payment, alice);
        fixture.useHistoricalClock();
        BlockData local = repository.getBlockRepository().getLastBlock();
        assertFalse(Arrays.equals(local.getSignature(), fixture.blocks.get(0).getSignature()));
        Controller.getInstance().refillLatestBlocksCache();
        repository.discardChanges();
        return local;
    }

    private static Object fetchReorg(Repository repository, Fixture fixture, Peer peer) throws Exception {
        Method method = Synchronizer.class.getDeclaredMethod("fetchReorgPlan", Repository.class,
                Peer.class, BlockData.class, int.class, List.class);
        Object plan = invoke(method, repository, peer, fixture.base,
                fixture.blocks.get(fixture.blocks.size() - 1).getHeight(), fixture.summaries);
        return FieldUtils.readField(plan, "result", true);
    }

    private static Synchronizer.SynchronizationResult append(Repository repository, ScriptedPeer peer,
            BlockData tip, List<BlockSummaryData> summaries) throws Exception {
        Method method = Synchronizer.class.getDeclaredMethod("synchronizeAppendSession", Repository.class,
                java.util.concurrent.locks.ReentrantLock.class, Peer.class, BlockData.class,
                int.class, BlockData.class, List.class);
        return (Synchronizer.SynchronizationResult) invoke(method, repository,
                Controller.getInstance().getBlockchainLock(), peer, tip,
                peer.fixture.blocks.get(2).getHeight(), tip, summaries);
    }

    private static Object invoke(Method method, Object... arguments) throws Exception {
        method.setAccessible(true);
        try {
            return method.invoke(Synchronizer.getInstance(), arguments);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof Exception) throw (Exception) e.getCause();
            throw e;
        }
    }

    private static void assertCommittedTip(Repository repository, BlockData expected) throws DataException {
        repository.discardChanges();
        try (Repository reader = RepositoryManager.getRepository()) {
            BlockData actual = reader.getBlockRepository().getLastBlock();
            assertEquals(expected.getHeight(), actual.getHeight());
            assertArrayEquals(expected.getSignature(), actual.getSignature());
        }
    }

    private static final class Fixture {
        private final BlockData base;
        private final String atAddress;
        private final List<BlockData> blocks = new ArrayList<>();
        private final List<BlockSummaryData> summaries = new ArrayList<>();
        private final List<byte[]> messages = new ArrayList<>();
        private final List<byte[]> stateHashes = new ArrayList<>();

        private Fixture(Repository repository) throws Exception {
            this(repository, 3);
        }

        private Fixture(Repository repository, int count) throws Exception {
            atAddress = AtUtils.doDeployAT(repository, Common.getTestAccount(repository, "alice"),
                    AtUtils.buildSimpleAT(), 1_00000000L).getATAccount().getAddress();
            base = repository.getBlockRepository().getLastBlock();
            for (int i = 0; i < count; ++i) {
                Block block = BlockUtils.mintBlock(repository);
                // Read a separate BlockData so orphaning the fixture cannot clear its height.
                BlockData data = repository.getBlockRepository().getLastBlock();
                blocks.add(data);
                summaries.add(new BlockSummaryData(data));
                messages.add((byte[]) FieldUtils.readField(new BlockV2Message(block), "dataBytes", true));
                stateHashes.add(repository.getATRepository().getLatestATState(atAddress).getStateHash());
            }
            BlockUtils.orphanBlocks(repository, count);
            Controller.getInstance().refillLatestBlocksCache();
            // Exercise historical sync. Test minting uses cached synthetic online-account
            // nonces, which must be outside the signature-retention window when replayed.
            useHistoricalClock();
        }

        private void useHistoricalClock() {
            NTP.setFixedOffset(blocks.get(blocks.size() - 1).getTimestamp()
                    + BlockChain.getInstance().getOnlineAccountSignaturesMinLifetime() + 1_000L
                    - System.currentTimeMillis());
        }
    }

    @FunctionalInterface
    private interface DuringBatch {
        void run() throws DataException;
    }

    // Serve the actual wire formats. The advertised tip can be far beyond the retained
    // fixture, as on a live peer serving a bounded historical replacement segment.
    private static final class ReorgPeer extends Peer {
        private final Fixture fixture;
        private final List<BlockSummaryData> summaries = new ArrayList<>();
        private int batchRequests;
        private int blockRequests;
        private int[] batchReplySizes = {};
        private DuringBatch onBatch;

        private ReorgPeer(Fixture fixture) {
            super(new PeerData(new PeerAddress("127.0.0.1:12392")), Peer.NETWORK);
            this.fixture = fixture;
            this.summaries.add(new BlockSummaryData(fixture.base));
            this.summaries.addAll(fixture.summaries);
            setPeersVersion("qortal-6.1.9", 0x0600010009L);
            BlockData tip = fixture.blocks.get(fixture.blocks.size() - 1);
            setChainTipData(new BlockSummaryData(fixture.blocks.size() == 240 ? tip.getHeight() + 1000 : tip.getHeight(),
                    tip.getSignature(), tip.getMinterPublicKey(), NTP.getTime()));
        }

        private int indexAfter(byte[] parent) {
            if (Arrays.equals(parent, fixture.base.getSignature())) return 0;
            for (int i = 0; i < fixture.blocks.size(); ++i)
                if (Arrays.equals(parent, fixture.blocks.get(i).getSignature())) return i + 1;
            throw new AssertionError("Unknown parent requested");
        }

        @Override
        public Message getResponseWithTimeout(Message message, int timeout) {
            assertFalse(Controller.getInstance().getBlockchainLock().isHeldByCurrentThread());
            try {
                switch (message.getType()) {
                    case GET_BLOCK_SUMMARIES: {
                        List<BlockSummaryData> first = summaries.subList(0, Math.min(8, summaries.size()));
                        BlockSummariesV2Message reply = new BlockSummariesV2Message(first);
                        return BlockSummariesV2Message.fromByteBuffer(1, payload(reply));
                    }
                    case GET_SIGNATURES_V2: {
                        GetSignaturesV2Message request = (GetSignaturesV2Message)
                                GetSignaturesV2Message.fromByteBuffer(1, payload(message));
                        int start = indexAfter(request.getParentSignature());
                        List<byte[]> signatures = new ArrayList<>();
                        for (int i = start; i < Math.min(start + request.getNumberRequested(), fixture.blocks.size()); ++i)
                            signatures.add(fixture.blocks.get(i).getSignature());
                        return SignaturesMessage.fromByteBuffer(1, payload(new SignaturesMessage(signatures)));
                    }
                    case GET_BLOCKS: {
                        ++batchRequests;
                        GetBlocksMessage request = (GetBlocksMessage) message;
                        int start = indexAfter(request.getParentSignature());
                        int count = Math.min(request.getNumberRequested(), fixture.blocks.size() - start);
                        if (batchRequests <= batchReplySizes.length)
                            count = Math.min(count, batchReplySizes[batchRequests - 1]);
                        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                        new DataOutputStream(bytes).writeInt(count);
                        for (int i = start; i < start + count; ++i)
                            bytes.write(fixture.messages.get(i));
                        if (onBatch != null) onBatch.run();
                        return BlocksMessage.fromByteBuffer(1, ByteBuffer.wrap(bytes.toByteArray()));
                    }
                    case GET_BLOCK: {
                        ++blockRequests;
                        ByteBuffer request = payload(message);
                        byte[] signature = new byte[request.remaining()];
                        request.get(signature);
                        int index = indexAfter(signature) - 1;
                        return BlockV2Message.fromByteBuffer(1, ByteBuffer.wrap(fixture.messages.get(index)));
                    }
                    default:
                        throw new AssertionError("Unexpected request " + message.getType());
                }
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }

        private static ByteBuffer payload(Message message) throws IllegalAccessException {
            return ByteBuffer.wrap((byte[]) FieldUtils.readField(message, "dataBytes", true));
        }
    }

    private static final class ScriptedPeer extends Peer {
        private final Fixture fixture;
        private final int availableBlocks;
        private int batchRequests;
        private int blockRequests;
        private int signatureRequests;
        private boolean wrongSecondBlock;
        private Runnable onMissingBlock;

        private ScriptedPeer(Fixture fixture, int availableBlocks) {
            super(new PeerData(new PeerAddress("127.0.0.1:12392")), Peer.NETWORK);
            this.fixture = fixture;
            this.availableBlocks = availableBlocks;
            setPeersVersion("qortal-6.1.9", 0x0600010009L);
        }

        private void useLegacyVersion() {
            setPeersVersion("qortal-5.4.0", 0x0500040000L);
        }

        @Override
        public Message getResponseWithTimeout(Message message, int timeout) {
            assertFalse("Network requests must not hold the blockchain lock",
                    Controller.getInstance().getBlockchainLock().isHeldByCurrentThread());
            if (message.getType() == MessageType.GET_BLOCKS) {
                ++batchRequests;
                return new BlocksMessage(Collections.emptyList());
            }
            if (message.getType() == MessageType.GET_SIGNATURES_V2) {
                ++signatureRequests;
                return null;
            }
            assertEquals(MessageType.GET_BLOCK, message.getType());
            ++blockRequests;
            byte[] signature;
            try {
                signature = (byte[]) FieldUtils.readField(message, "dataBytes", true);
            } catch (IllegalAccessException e) {
                throw new AssertionError(e);
            }
            for (int i = 0; i < fixture.blocks.size(); ++i) {
                if (!Arrays.equals(signature, fixture.blocks.get(i).getSignature())) continue;
                if (i >= availableBlocks) {
                    if (onMissingBlock != null) onMissingBlock.run();
                    return null;
                }
                try {
                    int responseIndex = wrongSecondBlock && blockRequests == 2 ? 0 : i;
                    return BlockV2Message.fromByteBuffer(1, ByteBuffer.wrap(fixture.messages.get(responseIndex)));
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            }
            fail("Unexpected block signature");
            return null;
        }
    }
}
