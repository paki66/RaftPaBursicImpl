package hr.fipu.raft.utils;

import java.io.IOException;
import java.net.Socket;

public class Connection implements Runnable {
    private final String host;
    private final int port;
    private Socket socket;

    public Connection(String host, int port) {
        this.port = port;
        this.host = host;
    }

    @Override
    public synchronized void run() {
        while (this.socket == null || !this.socket.isConnected()) {
            try {
                this.socket = new Socket(host, port);
                System.out.println("Connected to server on port " + port);
                break;
            } catch (IOException e) {
                System.err.println("Failed to connect to server on port " + port + ": " + e.getMessage());
            }
            try {
                Thread.currentThread().wait(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Restore interrupted status
                System.err.println("Connection thread interrupted: " + e.getMessage());
            }
        }
    }

    public int getPort() {
        return port;
    }

    public Socket getSocket() {
        return socket;
    }
}
