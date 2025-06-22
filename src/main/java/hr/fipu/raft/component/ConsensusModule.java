package hr.fipu.raft.component;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class ConsensusModule {
    private final long port;
    private final Socket clientSocket;
    private final ObjectInputStream inputStream;
    private final ObjectOutputStream outputStream;
    private int nextIndex;
    private int matchIndex = 0;

    public ConsensusModule(long port,
                           Socket clientSocket,
                           ObjectInputStream inputStream,
                           ObjectOutputStream outputStream,
                           int leaderLastLogIndex) {
        this.port = port;
        this.clientSocket = clientSocket;
        this.inputStream = inputStream;
        this.outputStream = outputStream;
        this.nextIndex = leaderLastLogIndex + 1;
    }

    public long getPort() {
        return port;
    }

    public Socket getClientSocket() {
        return clientSocket;
    }

    public ObjectInputStream getInputStream() {
        return inputStream;
    }

    public ObjectOutputStream getOutputStream() {
        return outputStream;
    }
}
