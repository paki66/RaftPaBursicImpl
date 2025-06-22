package hr.fipu.raft.rpc;

import hr.fipu.raft.component.ConsensusModule;

import java.io.Serializable;

public interface RemoteProcedureCall extends Serializable {
    RpcResponse execute(ConsensusModule socket);
}
