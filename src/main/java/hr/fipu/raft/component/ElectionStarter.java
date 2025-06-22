package hr.fipu.raft.component;

import hr.fipu.raft.utils.ServerStatus;

public class ElectionStarter implements Runnable {
    private final RaftServer server;

    public ElectionStarter(RaftServer server) {
        this.server = server;
    }

    @Override
    public void run() {
        this.server.startElection();
        if (server.getStatus() == ServerStatus.LEADER) {
            this.server.sendHeartbeat();
        }
    }
}
