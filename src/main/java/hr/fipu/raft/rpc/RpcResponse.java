package hr.fipu.raft.rpc;

import java.io.Serializable;

public class RpcResponse implements Serializable {
    private final int term;
    private final boolean success;

    public RpcResponse(int term, boolean success) {
        this.term = term;
        this.success = success;
    }

    public int getTerm() {
        return term;
    }

    public boolean isSuccess() {
        return success;
    }
}
