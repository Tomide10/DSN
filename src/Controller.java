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
    private Map<Integer, Boolean> isDstore = new HashMap<>();
    private InetAddress localHost = InetAddress.getLocalHost();
    private final ArrayList<Socket> loadingDStores = new ArrayList<>();

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
        for (int key : rebalanceCommands.keySet()){
            if (Objects.equals(rebalanceCommands.get(key), "REBALANCE 0 0")){
                emptyKeys.add(key);
            }
        }
        for(int key: emptyKeys){
            rebalanceCommands.remove(key);
        }
        return rebalanceCommands;

    }

    public static void main(String[] args) throws UnknownHostException {
        Controller controller = new Controller(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]));
        controller.run();
    }

    private void runRebalance() {
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(rebalance);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                if (dstores.size() >= R && !rebalanceInProgress[0] && allStored()) {
                    System.out.println("Starting Rebalance");
                    rebalanceInProgress[0] = true;
                    try {
                        Map<Integer, String[]> fileAllocation = new TreeMap<>();
                        for (Integer port : portToDStore.keySet()) {
                            Socket dstore = portToDStore.get(port);
                            dStoreMessageOuts.get(port).println(Protocol.LIST_TOKEN);
                            dstore.setSoTimeout(timeout);
                            BufferedReader listIn = new BufferedReader(new InputStreamReader(dstore.getInputStream()));
                            String list = listIn.readLine();
                            if (Objects.equals(list, Protocol.LIST_TOKEN)) {
                                fileAllocation.put(port, new String[0]);
                                System.out.println("No files at Dstore "+port);
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
                                    dStoreMessageOuts.get(port).println(Protocol.REMOVE_TOKEN + " " + file);
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
                                        System.out.println("Recieved REBALANCE_COMLPETE. Waiting for "+ balanced.getCount() + " more.");
                                    }
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }).start();
                            dStoreMessageOuts.get(portKey).println(allocations.get(portKey));
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
            }
        }).start();
    }

    private boolean allStored() {
        for (String status : index.values()){
            if (Objects.equals(status, "store in progress")){
                return false;
            }
        }
        return true;
    }

    private void run() {
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
                                    PrintWriter messageOut = new PrintWriter(client.getOutputStream());
                                    handleMessages(client, messageIn, messageOut);
                                }catch (Exception e){
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
            if (!isDstore.get(client.getPort())) {
                String line;
                if ((line = messageIn.readLine()) != null) {
                    String[] message = line.split(" ");
                    switch (message[0]) {
                        case Protocol.JOIN_TOKEN -> {
                            dstores.add(client);
                            dStoreMessageOuts.put(Integer.parseInt(message[1]), new PrintWriter(new Socket(localHost, Integer.parseInt(message[1])).getOutputStream(), true));
                            portToDStore.put(Integer.parseInt(message[1]), client);
                            port = Integer.parseInt(message[1]);
                            isDstore.put(client.getPort(), true);
                            System.out.println("DStore joined on port " + Integer.parseInt(message[1]));
                        }
                        case Protocol.STORE_TOKEN -> store(message, messageOut);
                        case Protocol.LOAD_TOKEN -> load(message, messageOut);
                        case Protocol.REMOVE_TOKEN -> remove(message, messageOut);
                        case Protocol.LIST_TOKEN -> list(line, messageOut);
                        case Protocol.RELOAD_TOKEN -> reload(message, messageOut);

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
            } else {
                System.out.println("Detected Failed DStore.");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();

        }
    }

    private void store(String[] message, PrintWriter messageOut) throws InterruptedException {
        if (malformed(Protocol.STORE_TOKEN, message)) {
            StringBuilder line = new StringBuilder();
            for (String word : message) {
                line.append(" ").append(word);
            }
            badMessageLog.put(new Date().toString(), String.valueOf(line));
        } else if (dstores.size() < R) {
            messageOut.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            System.out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            messageOut.flush();
        } else if (index.containsKey(message[1])) {
            messageOut.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
            System.out.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
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
                while (intList.contains(num)) {
                    num = new Random().nextInt(portToDStore.size());
                }
                intList.add(num);
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
                messageOut.println(Protocol.STORE_COMPLETE_TOKEN);
                messageOut.flush();
                System.out.println("File stored: " + message[1]);
            } else {
                index.remove(message[1]);
                System.out.println("File couldn't be stored: " + message[1]);
            }
        }
    }

    private void load(String[] message, PrintWriter messageOut) {
        if (malformed(Protocol.LOAD_TOKEN, message)) {
            StringBuilder line = new StringBuilder();
            for (String word : message) {
                line.append(" ").append(word);
            }
            badMessageLog.put(new Date().toString(), String.valueOf(line));
        } else if (dstores.size() < R) {
            System.out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            messageOut.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            messageOut.flush();
        } else if (!index.containsKey(message[1])) {
            System.out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            messageOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            messageOut.flush();
        } else if (index.get(message[1]).equals("store in progress")) {
            System.out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            messageOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            messageOut.flush();
        } else if (index.get(message[1]).equals("remove in progress")) {
            System.out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            messageOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            messageOut.flush();
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
            messageOut.flush();
        }
    }

    private void reload(String[] message, PrintWriter messageOut) {

        if (malformed(Protocol.RELOAD_TOKEN, message)) {
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
            messageOut.println(Protocol.LOAD_FROM_TOKEN + " " + storePort + " " + filesizes.get(message[1]));
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
            System.out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            messageOut.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            messageOut.flush();
        } else if (!index.containsKey(message[1])) {
            System.out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            messageOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            messageOut.flush();
        } else if (index.get(message[1]).equals("store in progress")) {
            System.out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            messageOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            messageOut.flush();
        } else if (index.get(message[1]).equals("remove in progress")) {
            System.out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            messageOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
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
                            if (newLine.equals(Protocol.REMOVE_ACK_TOKEN + " " + message[1]) || newLine.equals(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + message[1])) {
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
                messageOut.println(Protocol.REMOVE_COMPLETE_TOKEN);
                messageOut.flush();
                System.out.println("Removed File: " + message[1]);
            }
        }
    }

    private void list(String line, PrintWriter messageOut) {
        if (!line.equals(Protocol.LIST_TOKEN)) {
            badMessageLog.put(new Date().toString(), line);
        } else if (dstores.size() < R) {
            System.out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            messageOut.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            messageOut.flush();
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
            messageOut.flush();
            System.out.println("Files Listed");
        }
    }

}
