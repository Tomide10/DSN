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
    private FileOutputStream logWriter;

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


    private void clearFolder(File folder){
        for (File file : folder.listFiles()) {
            if (file.isDirectory()) {
                clearFolder(file);
                file.delete();
            } else {
                file.delete();
            }
        }
    }

    public void run() throws IOException {
        File logFile = new File("Dstore" + port + "BadMessage.log");
        if (logFile.exists()){
            int n = 1;
            while (true){
                logFile = new File("Dstore" + port + "BadMessage" + n + ".log");
                if (!logFile.exists()){
                    break;
                }
                n++;
            }
        }
        logFile.createNewFile();
        logWriter = new FileOutputStream(logFile);
        ServerSocket listener = new ServerSocket(port);
        Socket controller = new Socket(localHost, cport);
        controllerOut = new PrintWriter(controller.getOutputStream(), true);
        controllerOut.println("JOIN " + port);
        System.out.println("Joining controller");
        controllerIn = new BufferedReader(new InputStreamReader(controller.getInputStream()));
        File outputFolder = new File(file_folder);
        if (outputFolder.listFiles() != null) {
            clearFolder(outputFolder);
        }

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
                    case Protocol.REMOVE_TOKEN -> {
                        if (malformed(Protocol.REMOVE_TOKEN, message)) {
                            logWriter.write((new Date() + ": " + line + "\n").getBytes());
                        } else {
                            System.out.println("Removing File: " + message[1]);
                            if (files.containsKey(message[1])) {
                                File file = new File(file_folder, message[1]);
                                 if (file.delete()) {
                                     files.remove(message[1]);
                                     controllerOut.println(Protocol.REMOVE_ACK_TOKEN + " " + message[1]);
                                     System.out.println("File Removed");
                                 }
                            } else {
                                controllerOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + ": " + message[1]);
                                System.out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + ": " + message[1]);
                            }
                        }
                    }
                    case Protocol.LIST_TOKEN -> {
                        if (malformed(Protocol.LIST_TOKEN, message)) {
                            logWriter.write((new Date() + ": " + line + "\n").getBytes());
                        } else {
                            System.out.println("Received List Message");
                            StringBuilder list = new StringBuilder(Protocol.LIST_TOKEN);
                            for (String filename : files.keySet()) {
                                list.append(" ").append(filename);
                            }
                            System.out.println("Sending Listed Message");
                            controllerOut.println(list);
                        }
                    }
                    case Protocol.REBALANCE_TOKEN  -> {
                        System.out.println("Rebalancing");
                        System.out.println("Message: " + Arrays.toString(message));
                        int buffer = 2;
                        for (int i = 0; i < Integer.parseInt(message[1]); i++) {
                            String file = message[buffer];
                            buffer++;
                            int stores = Integer.parseInt(message[buffer]);
                            for (int j = 0; j < stores; j++) {
                                buffer++;
                                Socket dstoreToSend = new Socket(InetAddress.getLocalHost(), Integer.parseInt(message[buffer]));
                                BufferedReader dStoreIn = new BufferedReader(new InputStreamReader(dstoreToSend.getInputStream()));
                                PrintWriter dStoreOut = new PrintWriter(dstoreToSend.getOutputStream(), true);
                                OutputStream fileOut = new BufferedOutputStream(dstoreToSend.getOutputStream());
                                System.out.println("Sending " + file + " to Dstore " + message[buffer]);
                                dStoreOut.println(Protocol.REBALANCE_STORE_TOKEN + " " + file + " " + files.get(file));
                                dstoreToSend.setSoTimeout(timeout);
                                String ack = dStoreIn.readLine();
                                if (Objects.equals(ack, Protocol.ACK_TOKEN)) {
                                    FileInputStream newFileIn = new FileInputStream(new File(file_folder, file));
                                    byte[] fileContent = newFileIn.readNBytes(Integer.parseInt(files.get(file)));
                                    fileOut.write(fileContent);
                                }
                            }
                            buffer++;
                        }
                        int stores = Integer.parseInt(message[buffer]);
                        for (int i = 0; i < stores; i++) {
                            buffer++;
                            files.remove(message[buffer]);
                        }

                        controllerOut.println(Protocol.REBALANCE_COMPLETE_TOKEN);
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
                    case Protocol.STORE_TOKEN -> {
                        if (malformed(Protocol.STORE_TOKEN, message)) {
                            logWriter.write((new Date() + ": " + line + "\n").getBytes());
                        } else {
                            System.out.println("Storing file: " + message[1]);
                            messageOut.println(Protocol.ACK_TOKEN);
                            client.setSoTimeout(timeout);
                            byte[] fileData = fileIn.readNBytes(Integer.parseInt(message[2]));
                            File outputFile = new File(file_folder, message[1]);
                            FileOutputStream fileOut = new FileOutputStream(outputFile);
                            fileOut.write(fileData);
                            fileOut.close();
                            files.put(message[1], message[2]);
                            controllerOut.println(Protocol.STORE_ACK_TOKEN + " " + message[1]);
                            System.out.println("File Stored");
                        }
                    }
                    case Protocol.LOAD_DATA_TOKEN -> {
                        if (malformed(Protocol.LOAD_DATA_TOKEN, message)) {
                            logWriter.write((new Date() + ": " + line + "\n").getBytes());
                        } else {
                            if (files.containsKey(message[1])) {
                                System.out.println("Loading file: " + message[1]);
                                File file = new File(file_folder, message[1]);
                                FileInputStream fileStream = new FileInputStream(file);
                                System.out.println("Getting file content");
                                byte[] fileContent = fileStream.readAllBytes();
                                System.out.println("Sending file content");
                                clientOut.write(fileContent);
                                System.out.println("File content sent");
                                fileStream.close();
                            } else {
                                client.close();
                            }
                        }
                    }
                    case Protocol.REBALANCE_STORE_TOKEN -> {
                        if (malformed(Protocol.REBALANCE_STORE_TOKEN, message)) {
                            logWriter.write((new Date() + ": " + line + "\n").getBytes());
                        } else {
                            messageOut.println("ACK");
                            client.setSoTimeout(timeout);
                            byte[] content = fileIn.readNBytes(Integer.parseInt(message[2]));
                            File outputFile = new File(file_folder, message[1]);
                            FileOutputStream fileOut = new FileOutputStream(outputFile);
                            fileOut.write(content);
                            fileOut.close();
                            files.put(message[1], message[2]);
                        }
                    }
                    case null, default -> logWriter.write((new Date() + ": " + line + "\n").getBytes());
                }
            }
        }

    }
}
