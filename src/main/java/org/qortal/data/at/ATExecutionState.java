package org.qortal.data.at;

/** Minimal parent snapshot for execution; no history join or payload materialization. */
public final class ATExecutionState {
    private final String address;
    private final int height;
    private final byte[] hash;
    private final long balance;

    public ATExecutionState(String address, int height, byte[] hash, long balance) {
        this.address = address;
        this.height = height;
        this.hash = hash.clone();
        this.balance = balance;
    }
    public String getAddress() { return address; }
    public long getBalance() { return balance; }
    public int getHeight() { return height; }
    public byte[] getHash() { return hash.clone(); }
}
