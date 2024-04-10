import java.io.*;
import java.net.*;
import java.util.*;

public class Dstore {
    final int port;
    final int cport;
    final int timeout;
    final String file_folder;
    Map<String, String> badMessageLog = new TreeMap<>();
    InetAddress localHost = InetAddress.getLocalHost();
    Map<String, String> files = new TreeMap<>();
    PrintWriter controllerOut;
    BufferedReader controllerIn;

    public Dstore(int port, int cport, int timeout, String file_folder) throws UnknownHostException {
        this.port = port;
        this.cport = cport;
        this.timeout = timeout;
        this.file_folder = file_folder;
    }

    public static boolean malformed(String action, String[] message) {
        switch (action) {
            case "JOIN" -> {
                return (message.length != 2);
            }

            case null, default -> {
                return false;
            }
        }
    }

    public static void main(String[] args) throws IOException {
        Dstore dstore = new Dstore(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]), args[3]);
        dstore.run();
    }

    public void run() throws IOException {
        ServerSocket listener = new ServerSocket(port);
        Socket controller = new Socket(localHost, cport);
        controllerOut = new PrintWriter(controller.getOutputStream());
        controllerOut.println("JOIN " + port);
        System.out.println("Joining controller");
        controllerOut.flush();
        Socket controllerInput = listener.accept();
        controllerIn = new BufferedReader(new InputStreamReader(controllerInput.getInputStream()));
        new Thread(() -> {
            try {
                handleControllerMessages();
            } catch (IOException e) {
                System.out.println("Connection with controller failed");
                System.out.println("Shutting Down Dstore");
                System.exit(0);
            }
        }).start();

        for (; ; ) {
            try {
                final Socket client = listener.accept();
                System.out.println("Accepted Connection");
                new Thread(() -> {
                    try {
                        handleMessages(client);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).start();

            } catch (SocketException e) {
                System.out.println("Can't Connect to Controller");
                System.out.println("Shutting down DStore");
                break;
            }
        }
    }

    private void handleControllerMessages() throws IOException {
        String line;
        while (true) {
            if ((line = controllerIn.readLine()) != null) {
                String[] message = line.split(" ");
                switch (message[0]) {
                    case "REMOVE" -> {
                        if (malformed("REMOVE", message)) {
                            badMessageLog.put(new Date().toString(), line);
                        } else {
                            System.out.println("Received Removed Message");
                            if (files.containsKey(message[1])) {
                                File file = new File(file_folder, message[1]);
                                 if (file.delete()) {
                                     files.remove(message[1]);
                                     controllerOut.println("REMOVE_ACK " + message[1]);
                                     controllerOut.flush();
                                     System.out.println("File Removed");
                                 }
                            } else {
                                controllerOut.println("ERROR_FILE_DOES_NOT_EXIST " + message[1]);
                                System.out.println("ERROR_FILE_DOES_NOT_EXIST: " + message[1]);
                                controllerOut.flush();
                            }
                        }
                    }
                    case "LIST" -> {
                        if (malformed("LIST", message)) {
                            badMessageLog.put(new Date().toString(), line);
                        } else {
                            System.out.println("Received List Message");
                            StringBuilder list = new StringBuilder("LIST");
                            for (String filename : files.keySet()) {
                                list.append(" ").append(filename);
                            }
                            System.out.println("Sending Listed Message");
                            controllerOut.println(list);
                            controllerOut.flush();
                        }
                    }
                    case "REBALANCE" -> {
                        int buffer = 2;
                        for (int i = 0; i < Integer.parseInt(message[1]); i++) {
                            String file = message[buffer];
                            buffer++;
                            for (int j = 0; j < Integer.parseInt(message[buffer]); j++) {
                                buffer++;
                                Socket dstoreToSend = new Socket(InetAddress.getLocalHost(), Integer.parseInt(message[buffer]));
                                BufferedReader dStoreIn = new BufferedReader(new InputStreamReader(dstoreToSend.getInputStream()));
                                PrintWriter dStoreOut = new PrintWriter(dstoreToSend.getOutputStream());
                                OutputStream fileOut = new BufferedOutputStream(dstoreToSend.getOutputStream());

                                dStoreOut.println("REBALANCE_STORE " + file + " " + files.get(file));
                                dStoreOut.flush();
                                dstoreToSend.setSoTimeout(timeout);
                                String ack = dStoreIn.readLine();
                                if (Objects.equals(ack, "ACK")) {
                                    FileInputStream newFileIn = new FileInputStream(file_folder + "/" + file);
                                    byte[] fileContent = newFileIn.readNBytes(Integer.parseInt(files.get(file)));
                                    fileOut.write(fileContent);
                                    newFileIn.close();
                                }
                            }
                            buffer++;
                        }
                        for (int i = 0; i < Integer.parseInt(message[buffer]); i++) {
                            buffer++;
                            files.remove(message[buffer]);
                        }
                        controllerOut.println("REBALANCE_COMPLETE");
                        controllerOut.flush();
                    }
                }
            }
        }
    }

    public void handleMessages(Socket client) throws IOException {
        BufferedReader messageIn = new BufferedReader(new InputStreamReader(client.getInputStream()));
        PrintWriter messageOut = new PrintWriter(client.getOutputStream(), true);
        InputStream fileIn = client.getInputStream();
        OutputStream clientOut = client.getOutputStream();
        String line;
        while (true) {
            if ((line = messageIn.readLine()) != null) {
                String[] message = line.split(" ");
                switch (message[0]) {
                    case "STORE" -> {
                        if (malformed("STORE", message)) {
                            badMessageLog.put(new Date().toString(), line);
                        } else {
                            var start = System.nanoTime();
                            System.out.println("Received Store Message");
                            messageOut.println("ACK");
                            client.setSoTimeout(timeout);
                            byte[] fileData = fileIn.readNBytes(Integer.parseInt(message[2]));
                            File outputFile = new File(file_folder, message[1]);
                            FileOutputStream fileOut = new FileOutputStream(outputFile);
                            fileOut.write(fileData);
                            fileOut.close();
                            files.put(message[1], message[2]);
                            controllerOut.println("STORE_ACK " + message[1]);
                            controllerOut.flush();
                            var end = System.nanoTime();
                            System.out.println("File content stored in " + outputFile);
                            System.out.println("Time Elapsed: " + (end-start)/100000);
                        }
                    }
                    case "LOAD_DATA" -> {
                        if (malformed("LOAD_DATA", message)) {
                            badMessageLog.put(new Date().toString(), line);
                        } else {
                            if (files.containsKey(message[1])) {
                                System.out.println("Received Load Message");
                                File file = new File(file_folder + "/" + message[1]);
                                FileInputStream fileStream = new FileInputStream(file);
                                System.out.println("Getting file content");
                                byte[] fileContent = fileStream.readAllBytes();
                                System.out.println("Sending file content");
                                clientOut.write(fileContent);
                                fileStream.close();
                            } else {
                                client.close();
                            }
                        }
                    }
                    case null, default -> badMessageLog.put(new Date().toString(), line);
                }
            }
        }

    }
}
