package org.qortal.data.at;

/** Execution-only receipt reusing a parent payload. Persistence checks the parent head before reuse. */
public final class ATStateReference extends ATStateData {
    private final int parentStateHeight;

    public ATStateReference(ATExecutionState parent, int height, long fees, Long cursor) {
        super(parent.getAddress(), height, null, parent.getHash(), fees, false, cursor);
        this.parentStateHeight = parent.getHeight();
    }
    public int getParentStateHeight() { return parentStateHeight; }
}
