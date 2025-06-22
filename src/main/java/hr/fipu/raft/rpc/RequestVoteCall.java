package hr.fipu.raft.rpc;

import hr.fipu.raft.component.ConsensusModule;

import java.io.*;

public class RequestVoteCall implements RemoteProcedureCall {
    private final long requestId;
    private final int term;
    private final long candidateId;
    private final int lastLogIndex;
    private final int lastLogTerm;


    public RequestVoteCall(final int term, final long candidateId, final int lastLogIndex, final int lastLogTerm) {
        this.requestId = System.currentTimeMillis();
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    @Override
    public RpcResponse execute(final ConsensusModule socket) {
        try {
            ObjectOutputStream outputStream = socket.getOutputStream();
            ObjectInputStream inputStream = socket.getInputStream();
            outputStream.writeObject(this);
            RpcResponse response = (RpcResponse) inputStream.readObject();
            System.out.println("RequestVoteCall sent to " + socket.getPort() + " with response: " + response.isSuccess());
            return response;
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Failed to send RequestVoteCall: " + e.getMessage() + " to " + socket.getPort());
            return new RpcResponse(-1, false);
        }
    }

    public long getRequestId() {
        return requestId;
    }

    public int getTerm() {
        return term;
    }

    public long getCandidateId() {
        return candidateId;
    }

    public int getLastLogIndex() {
        return lastLogIndex;
    }

    public int getLastLogTerm() {
        return lastLogTerm;
    }
}
