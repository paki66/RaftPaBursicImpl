package hr.fipu.raft.component;

import hr.fipu.raft.rpc.AppendEntriesCall;
import hr.fipu.raft.rpc.RequestVoteCall;
import hr.fipu.raft.rpc.RpcResponse;
import hr.fipu.raft.utils.ClientConnection;
import hr.fipu.raft.utils.ClusterConnection;
import hr.fipu.raft.utils.Entry;
import hr.fipu.raft.utils.ServerStatus;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.*;
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
    private Socket clientSocket;
    private List<Connection> consensusModule;
    private final int[] ports = {8080, 8081, 8082};

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
    }

    public static int calculateTimeout(long id) {
        long currentMillis = System.currentTimeMillis() + (id * 1000L);
        int rangeSize = 10000 - 6000;
        return (int) (currentMillis % rangeSize) + 6000;
    }

    public void reconnect(Connection connection) {
        this.consensusModule.remove(connection);
        try {
            connection.getSocket().close();
        } catch (IOException e) {
            System.err.println("Failed to close socket for server on port " + connection.getPort() + ": " + e.getMessage());
        }
        Connection connectionToReconnect = new Connection(connection.getHost(), connection.getPort(), this.port);
        this.consensusModule.add(connectionToReconnect);
        System.out.println("Re-establishing connection to server on port " + connection.getPort());
        connect(connectionToReconnect);
    }

    public void connect(Connection connection) {
//        Thread thread = new Thread(connection, "ConnectionThread-" + connection.getPort());
//        thread.start();
        connection.start();
        try {
            connection.join();
        } catch (InterruptedException e) {
            System.err.println("Failed to re-establish connection to server on port " + connection.getPort() + ": " + e.getMessage());
        }
    }

    public void startElection() {
        System.out.println("Starting election");
        this.status = ServerStatus.CANDIDATE;
        this.term++;
        this.votedFor = this.id;
        this.consensusModule = createConnections();

        System.out.println("Sending RequestVote RPC to other servers");
        RequestVoteCall request = new RequestVoteCall(this.term, this.id, -1, -1);

        int votesReceived = 1;

        for (Connection connection : this.consensusModule) {
            if (!connection.getSocket().isConnected()) {
                connect(connection);
            }
            RpcResponse response = request.execute(connection);
            if (response.isSuccess()) {
                System.out.println("Vote granted by server on port " + connection.getPort());
                votesReceived++;
            } else {
                System.out.println("Vote denied by server on port " + connection.getPort());
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
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            for (Connection socket : this.consensusModule) {
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
                    reconnect(socket);
                }
            }
        }
    }

    public RpcResponse grantVote(long candidateId, int term) {
        System.out.println("Received vote request from candidate " + candidateId + " in term " + term);

        boolean voteAvailable = this.votedFor == -1 || this.votedFor == candidateId;
        boolean candidateUpToDate = this.term <= term; // TODO check if candidate's log is at least as up-to-date as this server's log

        if (candidateUpToDate) {
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

    public ServerStatus getStatus() {
        return status;
    }

    private List<Connection> createConnections() {
        List<Connection> connections = new LinkedList<>();
        for (Integer port : this.ports) {
            if (port != this.port) {
                Connection connection = new Connection(this.ipAddress, port, this.port);
                connections.add(connection);
            }
        }
        return connections;
    }

    private boolean isPortOutsideCluster(int port) {
        for (int p : this.ports) {
            if (p == port) {
                return false; // Port is part of the cluster
            }
        }
        return true; // Port is outside the cluster
    }

    public void handleConnectionRequest() {
        try {
            int timeout = calculateTimeout((int) this.id);
            System.out.println(timeout + "ms");
            this.serverSocket.setSoTimeout(timeout);
            this.clientSocket = serverSocket.accept();
            int clientPort = clientSocket.getPort();

            if (isPortOutsideCluster(clientPort)) {
                System.out.println("Received request from port " + clientPort + " which is outside the cluster. Waiting to connect.");
                Thread clientThead = new Thread(new ClientConnection(5000, clientSocket));
                clientThead.start();
            } else {
                System.out.println("Received request from port " + clientPort + " which is part of the cluster. Handling request.");
                Thread leaderRequestHandlerThread = new Thread(new ClusterConnection(this, clientSocket));
                leaderRequestHandlerThread.start();
            }

        } catch (SocketTimeoutException se) {
            if (this.status == ServerStatus.LEADER) {
                System.out.println("Socket timeout while waiting for requests. No other leader to wait for.");
                return;
            }
            System.out.println("Socket timeout, no request received. Breaking out of the loop.");
            Thread requestHandlerThread = new Thread(new ElectionStarter(this));
            requestHandlerThread.start();
        } catch (IOException e) {
            System.err.println("Error handling leader requests: " + e.getMessage());
        }
    }


    public RpcResponse handleRequestVoteCall(RequestVoteCall voteRequest) {
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
