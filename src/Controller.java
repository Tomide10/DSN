import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;


public class Controller {
    final Map<String, ArrayList<Integer>> storingIndex = new TreeMap<>();
    final Map<String, String> index = new TreeMap<>();
    final Map<String, String> filesizes = new TreeMap<>();
    final ArrayList<Socket> dstores = new ArrayList<>();

    final Map<Integer, Socket> portToDStore = new TreeMap<>();

    final boolean[] rebalanceInProgress = {false};
    private final int cport;
    private final int R;
    private final int timeout;
    private final int rebalance;
    private Map<Integer, Boolean> isDstore = new HashMap<>();
    private final ArrayList<Socket> loadingDStores = new ArrayList<>();
    private FileOutputStream logWriter;

    public Controller(int cport, int r, int timeout, int rebalance) throws UnknownHostException {
        System.out.println("Starting Controller");
        this.cport = cport;
        this.R = r;
        this.timeout = timeout;
        this.rebalance = rebalance;
    }

    private boolean isntInteger(String num) {
        try {
            Integer.parseInt(num);
            return false;
        } catch (NumberFormatException e) {
            return true;
        }
    }

    private boolean malformed(String action, String[] message) {
        switch (action) {
            case Protocol.JOIN_TOKEN -> {
                return (message.length != 2 || isntInteger(message[1]));
            }
            case Protocol.LOAD_TOKEN, Protocol.RELOAD_TOKEN, Protocol.REMOVE_TOKEN -> {
                return (message.length != 2);
            }
            case Protocol.LIST_TOKEN -> {
                return (!Objects.equals(message[0], Protocol.LIST_TOKEN));
            }
            case Protocol.STORE_TOKEN -> {
                return (message.length != 3 || isntInteger(message[2]));
            }

            case null, default -> {
                return true;
            }
        }
    }


    private Integer maxKey(Map<Integer, ArrayList<String>> allocations) {
        int max = allocations.keySet().toArray(new Integer[0])[0];
        for (Integer key : allocations.keySet()) {
            if (allocations.get(key).size() > allocations.get(max).size()) {
                max = key;
            }
        }
        return max;
    }

    private Integer minKey(Map<Integer, ArrayList<String>> allocations) {
        int min = allocations.keySet().toArray(new Integer[0])[0];
        for (Integer key : allocations.keySet()) {
            if (allocations.get(key).size() > allocations.get(min).size()) {
                min = key;
            }
        }
        return min;
    }

    private Map<Integer, String> efficientRebalance(Map<Integer, String[]> fileAllocation, Map<String, String> index, int R) {
        int floor = Math.floorDiv(R * index.size(), portToDStore.size());
        int ceil = Math.ceilDiv(R * index.size(), portToDStore.size());
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

//-----------------------------------------------------------------------------------------
        for (int portKey : returnAllocations.keySet()) {
            for (String file : fileAllocation.get(portKey)) {
                returnAllocations.get(portKey).add(file);
            }
        }

        while (returnAllocations.get(maxKey(returnAllocations)).size() > ceil) {
            for (String file : returnAllocations.get(maxKey(returnAllocations))) {
                if (!returnAllocations.get(minKey(returnAllocations)).contains(file)) {
                    returnAllocations.get(minKey(returnAllocations)).add(file);
                    returnAllocations.get(maxKey(returnAllocations)).remove(file);
                    break;
                }
            }
        }

//-----------------------------------------------------------------------------------------

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
            StringBuilder command = new StringBuilder(Protocol.REBALANCE_TOKEN);
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
            StringBuilder command = new StringBuilder(Protocol.REBALANCE_TOKEN);
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
        ArrayList<Integer> emptyKeys = new ArrayList<>();
        for (int key : rebalanceCommands.keySet()) {
            if (Objects.equals(rebalanceCommands.get(key), "REBALANCE 0 0")) {
                emptyKeys.add(key);
            }
        }
        for (int key : emptyKeys) {
            rebalanceCommands.remove(key);
        }
        return rebalanceCommands;

    }

    public static void main(String[] args) throws IOException {
        Controller controller = new Controller(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]));
        controller.run();
    }


    private void runRebalance() {
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(rebalance);
                    doRebalance();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private void doRebalance() {
        new Thread(() -> {
            if (dstores.size() >= R && !rebalanceInProgress[0] && allStored()) {
                System.out.println("Starting Rebalance");
                rebalanceInProgress[0] = true;
                try {
                    Map<Integer, String[]> fileAllocation = new TreeMap<>();
                    for (Integer port : portToDStore.keySet()) {
                        Socket dstore = portToDStore.get(port);
                        new PrintWriter(portToDStore.get(port).getOutputStream(), true).println(Protocol.LIST_TOKEN);
                        dstore.setSoTimeout(timeout);
                        BufferedReader listIn = new BufferedReader(new InputStreamReader(dstore.getInputStream()));
                        String list = listIn.readLine();
                        if (Objects.equals(list, Protocol.LIST_TOKEN)) {
                            fileAllocation.put(port, new String[0]);
                            System.out.println("No files at Dstore " + port);
                        } else {
                            String[] files = list.replaceFirst("LIST ", "").split(" ");
                            System.out.println("Files at Dstore " + port + ": " + Arrays.toString(files));
                            fileAllocation.put(port, files);
                        }
                    }

                    for (String filename : index.keySet()) {
                        boolean found = false;
                        for (String[] files : fileAllocation.values()) {
                            for (String file : files) {
                                if (Objects.equals(file, filename)) {
                                    found = true;
                                    break;
                                }
                            }
                        }
                        if (!found) {
                            index.remove(filename);
                        }
                    }

                    for (Integer port : fileAllocation.keySet()) {
                        for (String file : fileAllocation.get(port)) {
                            if (!index.containsKey(file)) {
                                new PrintWriter(portToDStore.get(port).getOutputStream(), true).println(Protocol.REMOVE_TOKEN + " " + file);
                                BufferedReader portReader = new BufferedReader(new InputStreamReader(portToDStore.get(port).getInputStream()));
                                String message;
                                if (!Objects.equals(message = portReader.readLine(), Protocol.REMOVE_ACK_TOKEN + " " + file)) {
                                    System.out.println("Removed extra file '" + file + "' from index");
                                }
                            }
                        }
                    }


                    //Map<Integer, String> allocations = efficientRebalance(fileAllocation);
                    Map<Integer, String> allocations = inefficientRebalance(fileAllocation, index, R);
                    System.out.println("New File Allocations: " + allocations);
                    CountDownLatch balanced = new CountDownLatch(allocations.size());
                    for (Integer portKey : allocations.keySet()) {
                        new Thread(() -> {
                            try {
                                BufferedReader portReader = new BufferedReader(new InputStreamReader(portToDStore.get(portKey).getInputStream()));
                                if (Objects.equals(portReader.readLine(), Protocol.REBALANCE_COMPLETE_TOKEN)) {
                                    balanced.countDown();
                                    System.out.println("Recieved REBALANCE_COMLPETE. Waiting for " + balanced.getCount() + " more.");
                                }
                            } catch (IOException e) {
                                System.err.println("Error while Rebalancing");
                                throw new RuntimeException(e);
                            }
                        }).start();
                        new PrintWriter(portToDStore.get(portKey).getOutputStream(), true).println(allocations.get(portKey));
                    }

                    if (balanced.await(timeout, TimeUnit.MILLISECONDS)) {
                        System.out.println("Rebalance Complete");
                    } else {
                        System.out.println("Rebalance Failed");
                    }
                    rebalanceInProgress[0] = false;
                } catch (Exception e) {
                    System.out.println("Rebalance May Have Failed:");
                    e.printStackTrace();
                    rebalanceInProgress[0] = false;
                }
            }
        }).start();
    }


    private boolean allStored() {
        return index.values().stream().allMatch(s -> Objects.equals(s, "store complete"));
    }

    private void run() throws IOException {
        File logFile = new File("ControllerBadMessages.log");
        if (logFile.exists()){
            int n = 1;
            while (true){
                logFile = new File("ControllerBadMessages" + n + ".log");
                if (!logFile.exists()){
                    break;
                }
                n++;
            }
        }
        logFile.createNewFile();
        logWriter = new FileOutputStream(logFile);
        try {
            ServerSocket listen = new ServerSocket(cport);
            runRebalance();
            System.out.println("Controller started on port " + cport);
            for (; ; ) {
                try {
                    final Socket client = listen.accept();
                    System.out.println("Accepted new connection");
                    isDstore.put(client.getPort(), false);
                    new Thread(() -> {
                        while (true) {
                            if (!rebalanceInProgress[0]) {
                                try {
                                    BufferedReader messageIn = new BufferedReader(new InputStreamReader(client.getInputStream()));
                                    PrintWriter messageOut = new PrintWriter(client.getOutputStream(), true);
                                    handleMessages(client, messageIn, messageOut);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
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

    private void handleMessages(Socket client, BufferedReader messageIn, PrintWriter messageOut) {
        int port = -1;
        try {
            if (!isDstore.get(client.getPort()) && !rebalanceInProgress[0]) {
                String line;
                if ((line = messageIn.readLine()) != null) {
                    String[] message = line.split(" ");
                    switch (message[0]) {
                        case Protocol.JOIN_TOKEN -> {
                            dstores.add(client);
                            //dStoreMessageOuts.put(Integer.parseInt(message[1]), new PrintWriter(new Socket(localHost, Integer.parseInt(message[1])).getOutputStream(), true));
                            portToDStore.put(Integer.parseInt(message[1]), client);
                            port = Integer.parseInt(message[1]);
                            isDstore.put(client.getPort(), true);
                            System.out.println("DStore joined on port " + Integer.parseInt(message[1]));
                            doRebalance();
                        }
                        case Protocol.STORE_TOKEN -> store(message, messageOut);
                        case Protocol.LOAD_TOKEN -> load(message, messageOut);
                        case Protocol.REMOVE_TOKEN -> remove(message, messageOut);
                        case Protocol.LIST_TOKEN -> list(line, messageOut);
                        case Protocol.RELOAD_TOKEN -> reload(message, messageOut);

                        case null, default -> logWriter.write((new Date() + ": " + line + "\n").getBytes());

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
            } else {
                System.out.println("Detected Failed DStore.");
            }
        } catch (InterruptedException e) {
            System.err.println("Error while Handling Message");
            e.printStackTrace();

        }
    }

    private Integer smallDstore(ArrayList<Integer> ports) {
        ArrayList<Integer> allPorts = (ArrayList<Integer>) List.copyOf(portToDStore.keySet());
        int min = allPorts.getFirst();
        int n = 0;
        int lastMin = 10000000;
        while (ports.contains(min)){
            n++;
            min = allPorts.get(n);
        }

        for (Integer key : allPorts) {
            if (!ports.contains(key)) {
                int m = 0;
                for (String file : storingIndex.keySet()) {
                    if (storingIndex.get(file).contains(key)) {
                        m++;
                    }
                }
                if (m < lastMin) {
                    lastMin = m;
                    min = key;
                }
            }
        }
        return min;
    }
    private void store(String[] message, PrintWriter messageOut) throws InterruptedException, IOException {
        if (malformed(Protocol.STORE_TOKEN, message)) {
            StringBuilder line = new StringBuilder();
            for (String word : message) {
                line.append(" ").append(word);
            }
            logWriter.write((new Date() + ": " + line + "\n").getBytes());

        } else if (dstores.size() < R) {
            messageOut.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            System.out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN + ": " + dstores.size() + ". Can't Store File ");
        } else if (index.containsKey(message[1])) {
            messageOut.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
            System.out.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
        } else {
            while (!loadingDStores.isEmpty()) {
                loadingDStores.removeFirst();
            }
            index.put(message[1], "store in progress");
            System.out.println("Storing file: " + message[1]);

            ArrayList<Integer> intList = new ArrayList<>();

            for (int i = 0; i < R ; i++){
                intList.add(smallDstore(intList));
            }

            Integer[] ints = intList.toArray(new Integer[R]);
            System.out.println(Arrays.toString(ints));
            ArrayList<Socket> dStoresToSend = new ArrayList<>();
            StringBuilder toClient = new StringBuilder(Protocol.STORE_TO_TOKEN);
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
                            if (newLine.equals(Protocol.STORE_ACK_TOKEN + " " + message[1])) {
                                allDstores.countDown();
                                System.out.println("STORE_ACK Received. Waiting for " + (allDstores.getCount()) + " more");
                            }
                        }
                    } catch (IOException e) {
                        System.err.println("Error while Storing file: " + message[1]);
                    }
                }).start();
            }
            messageOut.println(toClient);
            System.out.println("Waiting for STORE_ACKs from " + R + " DStores");
            if (allDstores.await(timeout, TimeUnit.MILLISECONDS)) {
                System.out.println("All STORE_ACKs Received");

                storingIndex.put(message[1], new ArrayList<>());
                for (Integer num : ints) {
                    storingIndex.get(message[1]).add((Integer) ports[num]);
                }
                filesizes.put(message[1], message[2]);
                index.put(message[1], "store complete");
                messageOut.println(Protocol.STORE_COMPLETE_TOKEN);
                System.out.println("File stored: " + message[1]);
            } else {
                index.remove(message[1]);
                System.out.println("File couldn't be stored: " + message[1]);
            }
        }
    }

    private void load(String[] message, PrintWriter messageOut) throws IOException {
        if (malformed(Protocol.LOAD_TOKEN, message)) {
            StringBuilder line = new StringBuilder();
            for (String word : message) {
                line.append(" ").append(word);
            }
            logWriter.write((new Date() + ": " + line + "\n").getBytes());
        } else if (dstores.size() < R) {
            System.out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN + ": " + dstores.size() + ". Can't Load File ");
            messageOut.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        } else if (!index.containsKey(message[1])) {
            System.out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            messageOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
        } else if (index.get(message[1]).equals("store in progress")) {
            System.out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            messageOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
        } else if (index.get(message[1]).equals("remove in progress")) {
            System.out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            messageOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
        } else {
            while (!loadingDStores.isEmpty()) {
                loadingDStores.removeFirst();
            }
            System.out.println("Loading File: " + message[1]);
            Integer storePort = storingIndex.get(message[1]).getFirst();
            Socket store = portToDStore.get(storePort);
            loadingDStores.add(store);
            messageOut.println(Protocol.LOAD_FROM_TOKEN + " " + storePort + " " + filesizes.get(message[1]));
            System.out.println("Loading file from port: " + storePort);
        }
    }

    private void reload(String[] message, PrintWriter messageOut) throws IOException {
        if (malformed(Protocol.RELOAD_TOKEN, message)) {
            StringBuilder line = new StringBuilder();
            for (String word : message) {
                line.append(" ").append(word);
            }
            logWriter.write((new Date() + ": " + line + "\n").getBytes());
        }
        Socket store = null;
        Integer port = null;
        for (int i = 0; i < storingIndex.get(message[1]).size(); i++) {
            if (!loadingDStores.contains(portToDStore.get(storingIndex.get(message[1]).get(i)))) {
                System.out.println("Attempt " + i + " at loading " + message[1]);
                port = storingIndex.get(message[1]).get(i);
                store = portToDStore.get(port);
                break;
            }
        }
        if (store == null) {
            System.out.println("Couldn't load file: " + message[1]);
            messageOut.println(Protocol.ERROR_LOAD_TOKEN);
        } else {
            loadingDStores.add(store);
            System.out.println("Loading file from port: " + port);
            messageOut.println(Protocol.LOAD_FROM_TOKEN + " " + port + " " + filesizes.get(message[1]));
        }
    }

    private void remove(String[] message, PrintWriter messageOut) throws IOException, InterruptedException {
        if (malformed(Protocol.REMOVE_TOKEN, message)) {
            StringBuilder line = new StringBuilder();
            for (String word : message) {
                line.append(" ").append(word);
            }
            logWriter.write((new Date() + ": " + line + "\n").getBytes());
        } else if (dstores.size() < R) {
            System.out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN + ": " + dstores.size() + ". Can't Remove File ");
            messageOut.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        } else if (!index.containsKey(message[1])) {
            System.out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            messageOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
        } else if (index.get(message[1]).equals("store in progress")) {
            System.out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            messageOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
        } else if (index.get(message[1]).equals("remove in progress")) {
            System.out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            messageOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
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
                            if (newLine.equals(Protocol.REMOVE_ACK_TOKEN + " " + message[1]) || newLine.equals(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + message[1])) {
                                allDstores.countDown();
                                System.out.println("REMOVE_ACK Received. Waiting for " + allDstores.getCount() + " more.");
                            }
                        }
                    } catch (IOException e) {
                        System.err.println("Error while removing file: " + message[1]);
                    }
                }).start();
            }
            for (Integer dStorePort : storingIndex.get(message[1])) {
                new PrintWriter(portToDStore.get(dStorePort).getOutputStream(), true).println(Protocol.REMOVE_TOKEN + " " + message[1]);
            }
            System.out.println("Waiting for REMOVE_ACKs from " + storingIndex.get(message[1]).size() + " DStores");
            if (allDstores.await(timeout, TimeUnit.MILLISECONDS)) {
                System.out.println("All REMOVE_ACKs Received");
                index.remove(message[1]);
                storingIndex.remove(message[1]);
                filesizes.remove(message[1]);
                messageOut.println(Protocol.REMOVE_COMPLETE_TOKEN);
                System.out.println("Removed File: " + message[1]);
            }
        }
    }

    private void list(String line, PrintWriter messageOut) throws IOException {
        if (!line.equals(Protocol.LIST_TOKEN)) {
            logWriter.write((new Date() + ": " + line + "\n").getBytes());
        } else if (dstores.size() < R) {
            System.out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN + ": " + dstores.size() + ". Can't List Files ");
            messageOut.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        } else {
            while (!loadingDStores.isEmpty()) {
                loadingDStores.removeFirst();
            }
            System.out.println("Listing Stored Files");
            StringBuilder list = new StringBuilder(Protocol.LIST_TOKEN);
            for (String filename : index.keySet()) {
                if (Objects.equals(index.get(filename), "store complete")) {
                    list.append(" ").append(filename);
                    System.out.println("File: " + filename);
                }
            }
            messageOut.println(list);
            System.out.println("Files Listed");
        }
    }

}
