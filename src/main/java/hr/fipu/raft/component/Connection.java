package hr.fipu.raft.component;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

public class Connection implements Runnable {
    private final String host;
    private final int port;
    private final Socket socket;
    private ObjectOutputStream out;
    private ObjectInputStream in;

    public Connection(String host, int port) {
        this.port = port;
        this.host = host;
        this.socket = new Socket();
    }

    @Override
    public synchronized void run() {
        while (!this.socket.isConnected()) {
            try {
                this.socket.connect(new InetSocketAddress(host, port), 3000);
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
        while (out == null || in == null) {
            try {
                this.out = new ObjectOutputStream(socket.getOutputStream());
                this.in = new ObjectInputStream(socket.getInputStream());
            } catch (IOException | NullPointerException e) {
                System.err.println("Failed to create input/output streams: " + e.getMessage());
            }
            try {
                Thread.currentThread().wait(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Restore interrupted status
                System.err.println("Connection thread interrupted: " + e.getMessage());
            }
        }
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public Socket getSocket() {
        return socket;
    }

    public ObjectOutputStream getOutputStream() {
        return out;
    }

    public ObjectInputStream getInputStream() {
        return in;
    }
}
