package hr.fipu.raft.utils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class ClientConnection extends Thread {
    private final int timeout;
    private final Socket socket;

    public ClientConnection(int timeout, Socket socket) {
        this.timeout = timeout;
        this.socket = socket;
    }

    @Override
    public synchronized void run() {
        try {
            ObjectOutputStream out = new ObjectOutputStream(this.socket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(this.socket.getInputStream());
            Object request = in.readObject();
            System.out.println("Received request: " + request);
            out.writeObject("Request processed successfully");
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Failed to process request: " + e.getMessage());
        } finally {
            try {
                this.socket.close();
                Thread.currentThread().join();
            } catch (IOException | InterruptedException e) {
                System.err.println("Failed to close socket: " + e.getMessage());
            }
        }
    }
}
