package hr.fipu.raft.rpc;

import hr.fipu.raft.component.Connection;
import hr.fipu.raft.utils.Entry;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class AppendEntriesCall implements RemoteProcedureCall {
    private final int term;
    private final long leaderId;
    private final Entry entry;
    private final int prevLogIndex;
    private final int prevLogTerm;
    private final int leaderCommit;

    public AppendEntriesCall(int term, long leaderId, Entry entry, int prevLogIndex, int prevLogTerm, int leaderCommit) {
        this.term = term;
        this.leaderId = leaderId;
        this.entry = entry;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.leaderCommit = leaderCommit;
    }

    @Override
    public RpcResponse execute(final Connection socket) {
        try {
            ObjectOutputStream outputStream = socket.getOutputStream();
            ObjectInputStream inputStream = socket.getInputStream();
            outputStream.writeObject(this);
            RpcResponse response = (RpcResponse) inputStream.readObject();
            System.out.println("AppendEntriesCall sent to " + socket.getPort() + " with response: " + response.isSuccess());
            return response;
        } catch (IOException | ClassNotFoundException | NullPointerException e) {
            System.err.println("Failed to send AppendEntriesCall: " + e.getMessage());
            return new RpcResponse(-1, false);
        }
    }

    public int getTerm() {
        return term;
    }

    public long getLeaderId() {
        return leaderId;
    }

    public Entry getEntry() {
        return entry;
    }

    public int getPrevLogIndex() {
        return prevLogIndex;
    }

    public int getPrevLogTerm() {
        return prevLogTerm;
    }

    public int getLeaderCommit() {
        return leaderCommit;
    }
}