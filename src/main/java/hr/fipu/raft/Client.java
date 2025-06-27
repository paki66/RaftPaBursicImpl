package hr.fipu.raft;

public class Client {

    public static void main(String[] args) {
        int port = Integer.parseInt(args[0]);
        String command = args[1];

        try (java.net.Socket socket = new java.net.Socket("localhost", port);
             java.io.ObjectOutputStream outputStream = new java.io.ObjectOutputStream(socket.getOutputStream());
             java.io.ObjectInputStream inputStream = new java.io.ObjectInputStream(socket.getInputStream())) {

            // Send the command to the server
            outputStream.writeObject(command);
            outputStream.flush();

            // Read the response from the server
            String response = (String) inputStream.readObject();
            System.out.println("Response from server: " + response);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
