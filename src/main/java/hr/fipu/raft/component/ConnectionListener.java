package hr.fipu.raft.component;

public class ConnectionListener extends Thread {
    private final RaftServer server;

    public ConnectionListener(RaftServer server) {
        this.server = server;
    }

    @Override
    public void run() {
        this.server.handleConnectionRequest();
    }

}
