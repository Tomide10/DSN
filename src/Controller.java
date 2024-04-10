import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class Controller {
    final Map<String, ArrayList<Integer>> storingIndex = new TreeMap<>();
    final Map<String, String> index = new TreeMap<>();
    final Map<String, String> filesizes = new TreeMap<>();
    final ArrayList<Socket> dstores = new ArrayList<>();

    final Map<Integer, Socket> portToDStore = new TreeMap<>();

    final Map<String, String> badMessageLog = new TreeMap<>();
    final boolean[] rebalanceInProgress = {false};
    private final int cport;
    private final int R;
    private final int timeout;
    private final int rebalance;
    private final Map<Integer, PrintWriter> dStoreMessageOuts = new TreeMap<>();
    private Map<Integer, Boolean>  isDstore = new HashMap<>();
    private InetAddress localHost = InetAddress.getLocalHost();
    private final ArrayList<Socket> loadingDStores = new ArrayList<>();
    private final int[] receivedAcks = {0};

    public Controller(int cport, int r, int timeout, int rebalance) throws UnknownHostException {
        System.out.println("Starting Controller");
        this.cport = cport;
        this.R = r;
        this.timeout = timeout;
        this.rebalance = rebalance;
    }

    private boolean isInteger(String num) {
        try {
            Integer.parseInt(num);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private boolean malformed(String action, String[] message) {
        switch (action) {
            case "JOIN" -> {
                return (message.length != 2 || !isInteger(message[1]));
            }
            case "LOAD" -> {
                return (message.length != 2);
            }
            case "REMOVE" -> {
                return (message.length != 2);
            }
            case "LIST" -> {
                return (!Objects.equals(message[0], "LIST"));
            }
            case "STORE" -> {
                return (message.length != 3 || !isInteger(message[2]));
            }
            case "RELOAD" -> {
                return (message.length != 2);
            }

            case null, default -> {
                return true;
            }
        }
    }

    private Map<Integer, String> inefficientRebalance(Map<Integer, String[]> fileAllocation, Map<String, String> index, int R) {
        ArrayList<String> addingFiles = new ArrayList<>();
        for (String filename : index.keySet()) {
            for (int i = 0; i < R; i++) {
                addingFiles.add(filename);
            }
        }
        Map<Integer, ArrayList<String>> returnAllocations = new TreeMap<>();
        for (int port : fileAllocation.keySet()) {
            returnAllocations.put(port, new ArrayList<>());
        }
        Integer[] ports = returnAllocations.keySet().toArray(new Integer[0]);
        for (int i = 0; i < addingFiles.size(); i++) {
            returnAllocations.get(ports[i % ports.length]).add(addingFiles.get(i));
        }

        //The rest should be included in the efficient rebalance. Calculating the returnAllocations is the only part
        // which is done in an inefficient way


        Map<Integer, Map<String, ArrayList<Integer>>> filesToSend = new TreeMap<>();
        Map<Integer, ArrayList<String>> filesToRemove = new TreeMap<>();

        for (int port : returnAllocations.keySet()) {
            filesToSend.put(port, new TreeMap<>());
            filesToRemove.put(port, new ArrayList<>());
        }

        for (Integer port : returnAllocations.keySet()) {
            for (String file : returnAllocations.get(port)) {
                if (Arrays.stream(fileAllocation.get(port)).noneMatch(s -> Objects.equals(s, file))) {
                    for (Integer newPort : fileAllocation.keySet()) {
                        if (Arrays.asList(fileAllocation.get(newPort)).contains(file)) {
                            if (filesToSend.get(newPort).getOrDefault(file, null) == null) {
                                filesToSend.get(newPort).put(file, new ArrayList<>());
                            }
                            filesToSend.get(newPort).get(file).add(port);
                            break;
                        }
                    }
                }
            }
        }


        for (Integer port : fileAllocation.keySet()) {
            for (String file : fileAllocation.get(port)) {
                if (!returnAllocations.get(port).contains(file)) {
                    filesToRemove.get(port).add(file);
                }
            }
        }

        Map<Integer, String> rebalanceCommands = new TreeMap<>();
        for (int port : returnAllocations.keySet()) {
            StringBuilder command = new StringBuilder("REBALANCE");
            command.append(" ").append(filesToSend.get(port).size()); //Number of files to send
            for (String file : filesToSend.get(port).keySet()) {
                command.append(" ").append(file); //File to send
                command.append(" ").append(filesToSend.get(port).get(file).size()); // Number of ports to send file to
                for (Integer portToSend : filesToSend.get(port).get(file)) {
                    command.append(" ").append(portToSend); //Ports to send file to
                }
            }

            command.append(" ").append(filesToRemove.get(port).size()); // Number of files to remove
            for (String fileToRemove : filesToRemove.get(port)) {
                command.append(" ").append(fileToRemove);
            }
            rebalanceCommands.put(port, String.valueOf(command));
        }

        return rebalanceCommands;

    }

    public static void main(String[] args) throws UnknownHostException {
        Controller controller = new Controller(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]));
        controller.run();
    }

    private void run() {
        try {
            ServerSocket listen = new ServerSocket(cport);
            TimerTask rebalanceTask = new TimerTask() {
                @Override
                public void run() {
                    if (dstores.size() >= R && !rebalanceInProgress[0]) {
                        rebalanceInProgress[0] = true;
                        try {
                            Map<Integer, String[]> fileAllocation = new TreeMap<>();
                            for (Socket dstore : dstores) {
                                PrintWriter dStoreOut = new PrintWriter(dstore.getOutputStream());
                                dStoreOut.println(Protocol.LIST_TOKEN);
                                dStoreOut.flush();
                                dstore.setSoTimeout(timeout);
                                BufferedReader listIn = new BufferedReader(new InputStreamReader(dstore.getInputStream()));
                                String list = listIn.readLine();
                                String[] files = list.replaceFirst("LIST ", "").split(" ");
                                fileAllocation.put(dstore.getPort(), files);
                            }
                            for (String filename : index.keySet()) {
                                boolean found = false;
                                for (String[] files : fileAllocation.values()) {
                                    for (String file : files) {
                                        if (Objects.equals(file, filename)) {
                                            found = true;
                                        }
                                    }
                                }
                                if (!found) {
                                    index.remove(filename);
                                }
                            }
                            for (Integer fileKey : fileAllocation.keySet()) {
                                for (String file : fileAllocation.get(fileKey)) {
                                    if (!index.containsKey(file)) {
                                        for (Socket dstore : dstores) {
                                            if (dstore.getPort() == fileKey) {
                                                PrintWriter dOut = new PrintWriter(dstore.getOutputStream());
                                                dOut.println(Protocol.REMOVE_TOKEN+ " " + file);
                                                //Read REMOVE_ACK
                                            }
                                        }
                                    }
                                }
                            }

                            //Map<Integer, String> allocations = efficientRebalance(fileAllocation);
                            Map<Integer, String> allocations = inefficientRebalance(fileAllocation, index, R);
                            CountDownLatch balanced = new CountDownLatch(dstores.size());
                            new Thread(() -> {
                                int last = 0;
                                while (balanced.getCount() != 0) {
                                    for (int i = 0; i < receivedAcks[0] - last; i++) {
                                        balanced.countDown();
                                    }
                                    last = receivedAcks[0];
                                }
                            });
                            for (int port : allocations.keySet()) {
                                PrintWriter dOut = new PrintWriter(portToDStore.get(port).getOutputStream());
                                dOut.println(allocations.get(port));
                                dOut.flush();
                            }
                            balanced.await(timeout, TimeUnit.MILLISECONDS);

                        } catch (Exception e) {
                            rebalanceInProgress[0] = false;
                        }
                    }
                }
            };
            Timer rebalanceTimer = new Timer();
            //rebalanceTimer.schedule(rebalanceTask, 0, rebalance);
            System.out.println("Controller started on port " + cport);
            for (; ; ) {
                try {
                    final Socket client = listen.accept();
                    System.out.println("Accepted new connection");
                    isDstore.put(client.getPort(), false);
                    new Thread(() -> {
                        while (true) {
                            handleMessages(client);
                        }
                    }).start();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void handleMessages(Socket client) {
        int port = -1;
        try {
            if (!isDstore.get(client.getPort())) {
                BufferedReader messageIn = new BufferedReader(new InputStreamReader(client.getInputStream()));
                PrintWriter messageOut = new PrintWriter(client.getOutputStream());
                String line;
                if ((line = messageIn.readLine()) != null) {
                    String[] message = line.split(" ");
                    switch (message[0]) {
                        case "JOIN" -> {
                            dstores.add(client);
                            dStoreMessageOuts.put(Integer.parseInt(message[1]), new PrintWriter(new Socket(localHost, Integer.parseInt(message[1])).getOutputStream(), true));
                            portToDStore.put(Integer.parseInt(message[1]), client);
                            port = Integer.parseInt(message[1]);
                            isDstore.put(client.getPort(), true);
                            System.out.println("DStore joined on port " + Integer.parseInt(message[1]));
                        }
                        case "STORE" -> store(message, messageOut);
                        case "LOAD" -> load(message, messageOut);
                        case "REMOVE" -> remove(message, messageOut);
                        case "LIST" -> list(line, messageOut);
                        case "RELOAD" -> reload(message, messageOut);
                        case "REBALANCE_COMPLETE" -> receivedAcks[0]++;

                        case null, default -> badMessageLog.put(new Date().toString(), line);

                    }
                }
            }
        } catch (IOException e) {
            loadingDStores.remove(client);
            dstores.remove(client);
            if (port != -1) {
                for (String file : storingIndex.keySet()) {
                    storingIndex.get(file).remove(port);
                }
                portToDStore.remove(port);
                System.out.println("Detected Failed DStore. Removing DStore at port " + port);
            }else{
                System.out.println("Detected Failed DStore.");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();

        }
    }

    private void store(String[] message, PrintWriter messageOut) throws InterruptedException {
        if (malformed("STORE", message)) {
            StringBuilder line = new StringBuilder();
            for (String word : message) {
                line.append(" ").append(word);
            }
            badMessageLog.put(new Date().toString(), String.valueOf(line));
        } else if (dstores.size() < R) {
            messageOut.println("ERROR_NOT_ENOUGH_DSTORES");
            System.out.println("ERROR_NOT_ENOUGH_DSTORES");
            messageOut.flush();
        } else if (index.containsKey(message[1])) {
            messageOut.println("ERROR_FILE_ALREADY_EXISTS");
            System.out.println("ERROR_FILE_ALREADY_EXISTS");
            messageOut.flush();
        } else {
            while (!loadingDStores.isEmpty()) {
                loadingDStores.removeFirst();
            }
            index.put(message[1], "store in progress");
            System.out.println("Storing file: " + message[1]);
            ArrayList<Integer> intList = new ArrayList<>();
            for (int i = 0; i < R; i++) {
                var num = new Random().nextInt(portToDStore.size());
                while (intList.contains(num)){
                    num = new Random().nextInt(portToDStore.size());
                }
                intList.add(num);
            }
            Integer[] ints = intList.toArray(new Integer[R]);
            System.out.println(Arrays.toString(ints));
            ArrayList<Socket> dStoresToSend = new ArrayList<>();
            StringBuilder toClient = new StringBuilder("STORE_TO");
            System.out.print("Storing file to ports:");

            var ports = portToDStore.keySet().toArray();
            for (int i = 0; i < ints.length; i++) {
                toClient.append(" ");
                toClient.append(ports[ints[i]]);
                dStoresToSend.add(portToDStore.get(ports[ints[i]]));
                System.out.print(" " + ports[ints[i]]);
            }
            System.out.println();

            CountDownLatch allDstores = new CountDownLatch(R);

            for (Socket dstore : dStoresToSend) {
                new Thread(() -> {
                    try {
                        BufferedReader dstoreIn = new BufferedReader(new InputStreamReader(dstore.getInputStream()));
                        String newLine = dstoreIn.readLine();
                        if (newLine != null) {
                            if (newLine.equals("STORE_ACK " + message[1])) {
                                allDstores.countDown();
                                System.out.println("STORE_ACK Received. Waiting for " + (allDstores.getCount()) + " more");
                            }
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).start();
            }
            messageOut.println(toClient);
            System.out.println("Waiting for STORE_ACKs from " + R + " DStores");
            messageOut.flush();
            if (allDstores.await(timeout, TimeUnit.MILLISECONDS)) {
                System.out.println("All STORE_ACKs Received");
                index.put(message[1], "store complete");
                storingIndex.put(message[1], new ArrayList<>());
                for (Object port : ports) {
                    storingIndex.get(message[1]).add((Integer) port);
                }
                filesizes.put(message[1], message[2]);
                messageOut.println("STORE_COMPLETE");
                messageOut.flush();
                System.out.println("File stored: " + message[1]);
            } else {
                index.remove(message[1]);
                System.out.println("File couldn't be stored: " + message[1]);
            }
        }
    }

    private void load(String[] message, PrintWriter messageOut) {
        if (malformed("LOAD", message)) {
            StringBuilder line = new StringBuilder();
            for (String word : message) {
                line.append(" ").append(word);
            }
            badMessageLog.put(new Date().toString(), String.valueOf(line));
        } else if (dstores.size() < R) {
            System.out.println("ERROR_NOT_ENOUGH_DSTORES");
            messageOut.println("ERROR_NOT_ENOUGH_DSTORES");
            messageOut.flush();
        } else if (!index.containsKey(message[1])) {
            System.out.println("ERROR_FILE_DOES_NOT_EXIST");
            messageOut.println("ERROR_FILE_DOES_NOT_EXIST");
            messageOut.flush();
        } else if (index.get(message[1]).equals("store in progress")) {
            System.out.println("ERROR_FILE_DOES_NOT_EXIST");
            messageOut.println("ERROR_FILE_DOES_NOT_EXIST");
            messageOut.flush();
        } else if (index.get(message[1]).equals("remove in progress")) {
            System.out.println("ERROR_FILE_DOES_NOT_EXIST");
            messageOut.println("ERROR_FILE_DOES_NOT_EXIST");
            messageOut.flush();
        } else {
            while (!loadingDStores.isEmpty()) {
                loadingDStores.removeFirst();
            }
            System.out.println("Loading File: " + message[1]);
            Integer storePort = storingIndex.get(message[1]).getFirst();
            Socket store = portToDStore.get(storePort);
            loadingDStores.add(store);
            messageOut.println("LOAD_FROM " + storePort + " " + filesizes.get(message[1]));
            System.out.println("Loading file from port: " + storePort);
            messageOut.flush();
        }
    }

    private void reload(String[] message, PrintWriter messageOut) {

        if (malformed("RELOAD", message)) {
            StringBuilder line = new StringBuilder();
            for (String word : message) {
                line.append(" ").append(word);
            }
            badMessageLog.put(new Date().toString(), String.valueOf(line));
        }
        Socket store = null;
        for (int i = 0; i < storingIndex.size(); i++) {
            if (!loadingDStores.contains(portToDStore.get(storingIndex.get(message[1]).get(i)))) {
                store = portToDStore.get(storingIndex.get(message[1]).get(i));
                break;
            }
        }
        if (store == null) {
            System.out.println("Couldn't load file: " + message[1]);
            messageOut.println(Protocol.ERROR_LOAD_TOKEN);
        } else {
            loadingDStores.add(store);
            int storePort = store.getPort();
            System.out.println("Loading file from port: " + storePort);
            messageOut.println("LOAD_FROM " + storePort + " " + filesizes.get(message[1]));
        }
    }

    private void remove(String[] message, PrintWriter messageOut) throws IOException, InterruptedException {
        if (malformed(Protocol.REMOVE_TOKEN, message)) {
            StringBuilder line = new StringBuilder();
            for (String word : message) {
                line.append(" ").append(word);
            }
            badMessageLog.put(new Date().toString(), line.toString());
        } else if (dstores.size() < R) {
            System.out.println("ERROR_NOT_ENOUGH_DSTORES");
            messageOut.println("ERROR_NOT_ENOUGH_DSTORES");
            messageOut.flush();
        } else if (!index.containsKey(message[1])) {
            System.out.println("ERROR_FILE_DOES_NOT_EXIST");
            messageOut.println("ERROR_FILE_DOES_NOT_EXIST");
            messageOut.flush();
        } else if (index.get(message[1]).equals("store in progress")) {
            System.out.println("ERROR_FILE_DOES_NOT_EXIST");
            messageOut.println("ERROR_FILE_DOES_NOT_EXIST");
            messageOut.flush();
        } else if (index.get(message[1]).equals("remove in progress")) {
            System.out.println("ERROR_FILE_DOES_NOT_EXIST");
            messageOut.println("ERROR_FILE_DOES_NOT_EXIST");
            messageOut.flush();
        } else {
            while (!loadingDStores.isEmpty()) {
                loadingDStores.removeFirst();
            }
            System.out.println("Removing File: " + message[1]);
            index.put(message[1], "remove in progress");
            CountDownLatch allDstores = new CountDownLatch(storingIndex.get(message[1]).size());
            for (Integer dstoreport : storingIndex.get(message[1])) {
                new Thread(() -> {
                    try {
                        BufferedReader dstoreIn = new BufferedReader(new InputStreamReader(portToDStore.get(dstoreport).getInputStream()));
                        String newLine;
                        if ((newLine = dstoreIn.readLine()) != null) {
                            if (newLine.equals("REMOVE_ACK " + message[1]) || newLine.equals("ERROR_FILE_DOES_NOT_EXIST " + message[1])) {
                                allDstores.countDown();
                                System.out.println("REMOVE_ACK Received. Waiting for " + allDstores.getCount() + " more.");
                            }
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).start();
            }
            for (Integer dStorePort : storingIndex.get(message[1])) {
                dStoreMessageOuts.get(dStorePort).println("REMOVE " + message[1]);

            }
            System.out.println("Waiting for REMOVE_ACKs from " + storingIndex.get(message[1]).size() + " DStores");
            if (allDstores.await(timeout, TimeUnit.MILLISECONDS)) {
                System.out.println("All REMOVE_ACKs Received");
                index.remove(message[1]);
                storingIndex.remove(message[1]);
                filesizes.remove(message[1]);
                messageOut.println("REMOVE_COMPLETE");
                messageOut.flush();
                System.out.println("Removed File: " + message[1]);
            }
        }
    }

    private void list(String line, PrintWriter messageOut) {
        if (!line.equals("LIST")) {
            badMessageLog.put(new Date().toString(), line);
        } else if (dstores.size() < R) {
            System.out.println("ERROR_NOT_ENOUGH_DSTORES");
            messageOut.println("ERROR_NOT_ENOUGH_DSTORES");
            messageOut.flush();
        } else {
            while (!loadingDStores.isEmpty()) {
                loadingDStores.removeFirst();
            }
            System.out.println("Listing Stored Files");
            StringBuilder list = new StringBuilder("LIST");
            for (String filename : index.keySet()) {
                if (Objects.equals(index.get(filename), "store complete")) {
                    list.append(" ").append(filename);
                    System.out.println("File: " + filename);
                }
            }
            messageOut.println(list);
            messageOut.flush();
            System.out.println("Files Listed");
        }
    }

}
