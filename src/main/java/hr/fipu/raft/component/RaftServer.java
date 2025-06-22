package hr.fipu.raft.component;

import hr.fipu.raft.rpc.AppendEntriesCall;
import hr.fipu.raft.rpc.RequestVoteCall;
import hr.fipu.raft.rpc.RpcResponse;
import hr.fipu.raft.utils.Connection;
import hr.fipu.raft.utils.Entry;
import hr.fipu.raft.utils.ServerStatus;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class RaftServer {
    private final long id;
    private final String ipAddress;
    private final int port;
    private ServerStatus status;
    private int term;
    private final List<Entry> log;
    private long votedFor;
    private final ServerSocket serverSocket;
    private final List<ConsensusModule> consensusModule = new LinkedList<>();
    private final List<Integer> ports = new LinkedList<>(Arrays.asList(8080, 8081, 8082, 8083, 8084));
    private final List<Integer> disconnectedPorts;

    private final int commitIndex = 0;
    private final int lastApplied = 0;

    public RaftServer(final long id, final String ipAddress, final int port) {
        this.id = id;
        this.ipAddress = ipAddress;
        this.status = ServerStatus.FOLLOWER;
        this.port = port;
        this.term = 0;
        this.log = new LinkedList<>(); // TODO linked ili arrayList
        this.votedFor = -1;
        try {
            this.serverSocket = new ServerSocket(port);
            System.out.println("Server started on port " + port);
        } catch (Exception e) {
            throw new RuntimeException("Failed to start server on port " + port, e);
        }
        this.disconnectedPorts = new LinkedList<>(this.ports);
    }

    public void startElection() {
        System.out.println("Starting election");
        this.status = ServerStatus.CANDIDATE;
        this.term++;
        this.votedFor = this.id;
        disconnectedPorts.clear();
        disconnectedPorts.addAll(ports);
        establishConnections();

        System.out.println("Sending RequestVote RPC to other servers");
        RequestVoteCall request = new RequestVoteCall(this.term, this.id, -1, -1);

        int votesReceived = 1;

        for (ConsensusModule consensusModule : this.consensusModule) {
            RpcResponse response = request.execute(consensusModule);
            if (response.isSuccess()) {
                System.out.println("Vote granted by server on port " + consensusModule.getPort());
                votesReceived++;
            } else {
                System.out.println("Vote denied by server on port " + consensusModule.getPort());
            }
        }

        if (votesReceived > this.consensusModule.size() / 2) { // TODO replace with actual majority number
            becomeLeader();
            System.out.println("Became leader in term " + this.term + " with " + votesReceived + " votes");
        } else {
            fold();
            System.out.println("Election failed, folding back to follower");
        }
    }

    public void sendHeartbeat() {
        if (this.status != ServerStatus.LEADER) {
            System.out.println("Cannot send heartbeat, not a leader");
            return;
        }

        System.out.println("Sending heartbeat to followers in term " + this.term);
        AppendEntriesCall heartbeat = new AppendEntriesCall(this.term, this.id, null, -1, -1, -1);


        while (true) {
            if (this.consensusModule.isEmpty()) {
                System.out.println("No followers available, cannot send heartbeat");
                fold();
                return;
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            for (ConsensusModule socket : this.consensusModule) {
                RpcResponse response = heartbeat.execute(socket);
                if (response.isSuccess()) {
                    System.out.println("Heartbeat sent successfully to server on port " + socket.getPort());
                } else {
                    if (response.getTerm() > this.term) {
                        System.out.println("Received higher term from server on port " + socket.getPort() + ", stepping down");
                        this.term = response.getTerm();
                        fold();
                        return;
                    }
                    System.out.println("Failed to send heartbeat to server on port " + socket.getPort());
                }
            }
        }
    }

    public RpcResponse grantVote(long candidateId, int term) {
        System.out.println("Received vote request from candidate " + candidateId + " in term " + term);

        boolean voteAvailable = this.votedFor == -1 || this.votedFor == candidateId;
        boolean candidateUpToDate = this.term <= term; // TODO check if candidate's log is at least as up-to-date as this server's log

        if (candidateUpToDate && voteAvailable) {
            System.out.println("Granting vote for candidate " + candidateId + " in term " + term);
            this.term = term;
            this.votedFor = candidateId;
            return new RpcResponse(this.term, true);
        }

        System.out.println("Rejecting vote for candidate " + candidateId + " in term " + term + " (current term: " + this.term + ")");
        return new RpcResponse(this.term, false);
    }

    public RpcResponse appendEntries(AppendEntriesCall appendEntriesCall) {
        int callTerm = appendEntriesCall.getTerm();
        System.out.println("Received AppendEntriesCall from leader " + appendEntriesCall.getLeaderId() + " in term " + callTerm);

        if (callTerm < this.term) {
            System.out.println("Rejecting AppendEntriesCall, current term is higher");
            return new RpcResponse(this.term, false);
        }

        if (callTerm > this.term) {
            System.out.println("Stepping down to follower due to higher term");
            this.term = callTerm;
            fold();
            this.votedFor = -1;
            return new RpcResponse(this.term, true);
        }


        return new RpcResponse(this.term, true);
    }

    private void becomeLeader() {
        this.status = ServerStatus.LEADER;
    }

    private void fold() {
        this.status = ServerStatus.FOLLOWER;
    }

    public ServerSocket getServerSocket() {
        return serverSocket;
    }

    public ServerStatus getStatus() {
        return status;
    }

    private void establishConnections() {
        this.consensusModule.clear();
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        for (Integer port : this.disconnectedPorts) {
            if (port != this.port) {
                Connection connection = new Connection(this.ipAddress, port);
                Thread thread = new Thread(connection, "ConnectionThread-" + port);
                thread.start();
                try {
                    thread.join();
                    Socket socket = connection.getSocket();
                    if (socket == null || !socket.isConnected()) {
                        System.err.println("Failed to connect to server on port " + port);
                        continue;
                    }
                    ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                    ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
                    ConsensusModule consensusModule = new ConsensusModule(port, socket, inputStream, outputStream, this.log.size());
                    this.consensusModule.add(consensusModule);
                } catch (IOException | InterruptedException e) {
                    System.err.println("Failed to create streams for server on port " + port + ": " + e.getMessage());
                }

            }
        }
    }

    public void handleLeaderRequests() {
        try {
            Socket socket = serverSocket.accept();
            long currentMillis = System.currentTimeMillis() + (this.id * 100);
            int rangeSize = 10000 - 6000;
            int timeout = (int) (currentMillis % rangeSize) + 6000;
            System.out.println(timeout + "ms");
            socket.setSoTimeout(timeout);
            ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());

            while (true) {
                try {
                    Object request = inputStream.readObject();

                    if (request instanceof RequestVoteCall requestVoteCall) {
                        RpcResponse response = this.handleRequestVoteCall(requestVoteCall);
                        outputStream.writeObject(response);
                    } else if (request instanceof AppendEntriesCall appendEntriesRequest) {
                        RpcResponse response = this.appendEntries(appendEntriesRequest);
                        outputStream.writeObject(response);
                    } else {
                        System.out.println("Received unknown request type: " + request.getClass().getName());
                    }
                } catch (SocketTimeoutException se) {
                    System.out.println("Socket timeout, no request received. Breaking out of the loop.");
                    break;
                }
            }
        } catch (ClassNotFoundException | IOException e) {
            e.printStackTrace();
        }
    }

    private RpcResponse handleRequestVoteCall(RequestVoteCall voteRequest) {
        System.out.println("Received RequestVoteCall from term: " + voteRequest.getTerm() + " from candidate ID: " + voteRequest.getCandidateId());
        RpcResponse response = this.grantVote(voteRequest.getCandidateId(), voteRequest.getTerm());

        if (response.isSuccess()) {
            System.out.println("Vote granted to candidate ID: " + voteRequest.getCandidateId());
        } else {
            System.out.println("Vote not granted to candidate ID: " + voteRequest.getCandidateId());
        }

        return response;
    }

}
