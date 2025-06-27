package hr.fipu.raft;

import hr.fipu.raft.component.ConnectionListener;
import hr.fipu.raft.component.RaftServer;
import hr.fipu.raft.utils.ServerStatus;

public class Server2 {

    public static void main(String[] args) {
        if (args.length < 3) {
            throw new IllegalArgumentException("Wrong number of arguments");
        }

        final String host = args[0].isEmpty() ? "localhost" : args[0];
        final int port = args[1].isEmpty() ? 8081 : Integer.parseInt(args[1]);
        final long id = args[2].isEmpty() ? 2L : Long.parseLong(args[2]);


        RaftServer server = new RaftServer(id, host, port);

//        System.out.println("Starting Raft server with ID: " + id + ", Host: " + host + ", Port: " + port);


        while (true) {
            ConnectionListener listener = new ConnectionListener(server);
            Thread thread = new Thread(listener);
            thread.start();
            try {
                thread.join(); // Wait for the connection listener to finish
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Restore interrupted status
                break; // Exit the loop if interrupted
            }
            if (server.getStatus() == ServerStatus.LEADER) {
                server.sendHeartbeat();
            }
        }
    }
}
