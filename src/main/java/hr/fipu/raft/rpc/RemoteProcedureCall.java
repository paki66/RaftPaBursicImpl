package hr.fipu.raft.rpc;

import hr.fipu.raft.component.Connection;

import java.io.Serializable;

public interface RemoteProcedureCall extends Serializable {
    RpcResponse execute(Connection socket);
}
