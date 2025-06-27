package hr.fipu.raft.utils;

import hr.fipu.raft.component.ElectionStarter;
import hr.fipu.raft.component.RaftServer;
import hr.fipu.raft.rpc.AppendEntriesCall;
import hr.fipu.raft.rpc.RequestVoteCall;
import hr.fipu.raft.rpc.RpcResponse;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;

public class ClusterConnection extends Thread {
    private final RaftServer server;
    private final Socket socket;

    public ClusterConnection(RaftServer server, Socket socket) {
        this.server = server;
        this.socket = socket;
    }

    @Override
    public void run() {
            while (true) {
                try {
                    ObjectOutputStream outputStream = new ObjectOutputStream(this.socket.getOutputStream());
                    ObjectInputStream inputStream = new ObjectInputStream(this.socket.getInputStream());
                    Object request = inputStream.readObject();

                    if (request instanceof RequestVoteCall requestVoteCall) {
                        RpcResponse response = this.server.handleRequestVoteCall(requestVoteCall);
                        outputStream.writeObject(response);
                    } else if (request instanceof AppendEntriesCall appendEntriesRequest) {
                        RpcResponse response = this.server.appendEntries(appendEntriesRequest);
                        outputStream.writeObject(response);
                    } else {
                        System.out.println("Received unknown request type: " + request.getClass().getName());
                    }
                } catch (SocketTimeoutException se) {
                    System.out.println("Socket timeout, no request received. Breaking out of the loop.");
                    Thread electionStarter = new Thread(new ElectionStarter(this.server));
                    electionStarter.start();
                    break;
                } catch (IOException | ClassNotFoundException e) {
                    System.err.println("Problem with connection: " + e.getMessage());
                    try {
                        this.socket.close();
                        Thread.currentThread().join();
                    } catch (InterruptedException | IOException ex) {
                        Thread.currentThread().interrupt(); // Restore interrupted status
                        System.err.println("Connection thread interrupted: " + ex.getMessage());
                    }
                }
            }
    }
}
