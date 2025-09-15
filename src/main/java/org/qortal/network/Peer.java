package org.qortal.network;

import org.qortal.controller.Controller;
import org.qortal.data.block.BlockSummaryData;
import org.qortal.data.block.CommonBlockData;
import org.qortal.data.network.PeerData;
import org.qortal.network.message.Message;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

public interface Peer {

    boolean sendMessage(Message message);
    void disconnect(String reason);
    boolean isOutbound();
    void setChainTipSummaries(List<BlockSummaryData> chainTipSummaries);
    BlockSummaryData getChainTipData();
    void setChainTipData(BlockSummaryData chainTipData);
    PeerData getPeerData();
    String getPeerIndexString();

    // legacy from old Peer implementation as class (now IPPeer)
    public static final Pattern VERSION_PATTERN = Pattern.compile(Controller.VERSION_PREFIX
            + "(\\d{1,3})\\.(\\d{1,5})\\.(\\d{1,5})");
    Long getPeersVersion();
    default Handshake getHandshakeStatus(){
        return null;
    }
    default Long getLastPing() {
        return null;
    }
    default Long getConnectionTimestamp() {
        return null;
    }
    default Long getPeersConnectionTimestamp() {
        return null;
    }
    String getPeersVersionString();
    String getPeersNodeId();
    default UUID getPeerConnectionId() {
        return null;
    };
    long getConnectionEstablishedTime();
    Long getLastTooDivergentTime();
    // legacy (from) Handshake
    void setPeersConnectionTimestamp(long peersConnectionTimestamp);
    void setPeersVersion(String versionString, long version);
    boolean isAtLeastVersion(String minPeerVersion);
    default void setPeersPublicKey(byte[] peersPublicKey) { return; }
    default void setPeersChallenge(byte[] peersChallenge) { return; }
    default byte[] getOurChallenge() { return  null; }
    default byte[] getPeersPublicKey() { return null; }
    void setPeersNodeId(String nodeAddress);
    default byte[] getPeersChallenge() { return null; }
    boolean isStopping();
    default void setHandshakeStatus(Handshake handshake) { return; }
    // legacy (from) Synchronizer
    void setSyncInProgress(boolean b);
    boolean canUseCachedCommonBlockData();
    void setCommonBlockData(CommonBlockData cbd);
    CommonBlockData getCommonBlockData();
    void setLastTooDivergentTime(Long time);
    Message getResponse(Message getBlockSummariesMessage) throws InterruptedException;
    default InetSocketAddress getResolvedAddress() { return null; }
    Message getResponseWithTimeout(Message getArbitraryDataFileMessage, int arbitraryRequestTimeout) throws InterruptedException;
    default void setMaxConnectionAge(long l) { return; };
    default void readChannel() throws IOException { return; };
    default boolean writeChannel() throws IOException { return false; };
    void setLastPing(long l);
}

