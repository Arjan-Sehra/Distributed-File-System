import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

class Controller {
    int cport;
    int R;
    int timeout;
    int rebalance_period;

    /** filename and status for index **/
    ConcurrentHashMap<String,String> index = new ConcurrentHashMap<>();
    /** filenames and number of store acks **/
    ConcurrentHashMap<String, Integer> storeAcks = new ConcurrentHashMap<>();
    /** filenames and sockets it connects to **/
    ConcurrentHashMap<String, Socket> filenameSockets = new ConcurrentHashMap<>();
    /** filenames and number of remove acks **/
    ConcurrentHashMap<String, Integer> filenameRemoveAcks = new ConcurrentHashMap<>();
    /** port number and socket (keeps track of dstores) **/
    ConcurrentHashMap<Integer, Socket> portSocket = new ConcurrentHashMap<>();
    /** filename and filesize **/
    ConcurrentHashMap<String, Integer> filenameFilesize = new ConcurrentHashMap<>();
    /** port number and list of files **/
    ConcurrentHashMap<Integer, ConcurrentLinkedQueue<String>> portFiles = new ConcurrentHashMap<>();
    /** port numbers and a lock **/
    ConcurrentHashMap<Integer, Object> portLock = new ConcurrentHashMap<>();
    /** filename and counter for remove acks **/
    ConcurrentHashMap<String, CountDownLatch> filenameRemoveAcksCounter = new ConcurrentHashMap<>();
    /** filename and counter for store acks **/
    ConcurrentHashMap<String, CountDownLatch> filenameStoreAcksCounter = new ConcurrentHashMap<>();
    /** filenames and list of ports it uses **/
    ConcurrentHashMap<String, ConcurrentLinkedQueue<Integer>> filenamePort = new ConcurrentHashMap<>();

    public Controller(int cport, int R, int timeout, int rebalance_period) {
        this.cport = cport;
        this.R = R;
        this.timeout = timeout;
        this.rebalance_period = rebalance_period;
        initialise();
    }

    public static void main(String[] args) {
        Controller controller = new Controller(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]));
    }

    public void initialise() {
        ServerSocket ss = null;
        try {
            ss = new ServerSocket(cport);
            while (true) {
                try {
                    Socket client = ss.accept();
                    new Thread(new ControllerServiceThread(client)).start();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (ss != null)
                try {
                    ss.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }

    public class ControllerServiceThread implements Runnable {
        Socket client;

        ControllerServiceThread(Socket c) {
            client = c;
        }

        public void run() {
            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                String line;
                /** arr represents the broken up list of the protocol **/
                String[] arr;
                while ((line = in.readLine()) != null) {
                    System.out.println(line);
                    arr = line.split(" ");
                    /** If protocol is STORE **/
                    if (arr[0].equals(Protocol.STORE_TOKEN)) {
                        /** If file exists in index **/
                        if (index.get(arr[1]) != null && index.get(arr[1]).equals("store complete")) {
                            out.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                        } else {
                            if (portFiles.size() < R) {
                                out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                                return;
                            }

                            /** Sorts the list **/
                            List<Map.Entry<Integer, ConcurrentLinkedQueue<String>>> sortedList = portFiles.entrySet().stream()
                                    .sorted(Comparator.comparingInt(e -> e.getValue().size()))
                                    .limit(R)
                                    .collect(Collectors.toList());
                            StringBuilder output = new StringBuilder();

                            for (Map.Entry<Integer, ConcurrentLinkedQueue<String>> entry : sortedList) {
                                synchronized (portLock.computeIfAbsent(entry.getKey(), k -> new Object())) {
                                    int port = entry.getKey();
                                    output.append(port).append(" ");
                                    entry.getValue().add(arr[1]);
                                }
                            }
                            out.println(Protocol.STORE_TO_TOKEN + " " + output);
                            filenameStoreAcksCounter.put(arr[1], new CountDownLatch(R));
                            index.put(arr[1], "store in progress");
                            storeAcks.put(arr[1], 0);
                            filenameSockets.put(arr[1], client);
                            filenameFilesize.put(arr[1], Integer.valueOf(arr[2]));
                        }
                    /** If protocol is REMOVE **/
                    } else if (arr[0].equals(Protocol.REMOVE_TOKEN)) {
                        if (index.get(arr[1]) != null && index.get(arr[1]).equals("store complete")) {
                            filenameRemoveAcks.put(arr[1], 0);
                            removing(arr[1]);
                            index.remove(arr[1]);
                        } else {
                            out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                        }
                    /** If protocol is LOAD **/
                    } else if (arr[0].equals(Protocol.LOAD_TOKEN)) {
                        if (index.get(arr[1]) != null && index.get(arr[1]).equals("store complete")) {
                            loadOrReload(arr[1], client, false);
                        } else {
                            out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                        }
                    /** If protocol is RELOAD **/
                    } else if (arr[0].equals(Protocol.RELOAD_TOKEN)) {
                        if (index.get(arr[1]) != null && index.get(arr[1]).equals("store complete")) {
                            loadOrReload(arr[1], client, true);
                        } else {
                            out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                        }
                    /** If protocol is JOIN **/
                    } else if (arr[0].equals(Protocol.JOIN_TOKEN)) {
                        join(arr[1]);
                        portSocket.put(Integer.parseInt(arr[1]), client);
                        PrintWriter outt = new PrintWriter(client.getOutputStream(), true);
                        outt.println(Protocol.LIST_TOKEN);
                    /** If protocol is STORE ACK **/
                    } else if (arr[0].equals(Protocol.STORE_ACK_TOKEN)) {
                        synchronized (storeAcks) {
                            storeAcks.computeIfPresent(arr[1], (k, v) -> v + 1);
                            System.out.println(storeAcks);
                            if (storeAcks.get(arr[1]) == R) {
                                index.put(arr[1], "store complete");
                                storeComplete(arr[1]);
                            } else {
                                filenameStoreAcksCounter.get(arr[1]).countDown();
                            }
                        }
                    /** If protocol is REMOVE ACK **/
                    } else if (arr[0].equals(Protocol.REMOVE_ACK_TOKEN)) {
                        synchronized (filenameRemoveAcks) {
                            if (storeAcks.getOrDefault(arr[1], 0) < R) {
                                return;
                            }
                            filenameRemoveAcks.computeIfPresent(arr[1], (k, v) -> v + 1);
                            if (filenameRemoveAcks.get(arr[1]) == R) {
                                removeComplete(arr[1]);
                            } else {
                                filenameRemoveAcksCounter.get(arr[1]).countDown();
                            }
                        }
                    } else if (arr[0].equals(Protocol.LIST_TOKEN)) {
                        list(client);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    /** Sends REMOVE when removing a file **/
    private void removing(String fileName) throws IOException {
        for (Map.Entry<Integer, ConcurrentLinkedQueue<String>> entry : portFiles.entrySet()) {
            if (entry.getValue().contains(fileName)) {
                entry.getValue().remove(fileName);
                Socket dstore = portSocket.get(entry.getKey());
                PrintWriter out = new PrintWriter(dstore.getOutputStream(), true);
                out.println(Protocol.REMOVE_TOKEN + " " + fileName);
            }
        }
        filenameRemoveAcksCounter.put(fileName, new CountDownLatch(R));
    }

    /** Sends REMOVE COMPLETE when successfully removes file **/
    private void removeComplete(String fileName) throws IOException {
        Socket client = filenameSockets.get(fileName);
        PrintWriter out = new PrintWriter(client.getOutputStream(), true);
        index.put(fileName, "remove complete");
        filenamePort.remove(fileName);
        out.println(Protocol.REMOVE_COMPLETE_TOKEN);
    }
    /** Updates portFiles when Dstore joins **/
    private void join(String portStr) {
        int porter = Integer.parseInt(portStr);
        this.portFiles.put(porter, new ConcurrentLinkedQueue<>());
    }

    /** For testing purposes **/
    public void printIntegersWithSockets() {
        for (int integer : portSocket.keySet()) {
            Socket socket = portSocket.get(integer);
            System.out.println("Integer: " + integer + ", Socket: " + socket);
        }
    }
    /** Sends STORE COMPLETE when store is completed **/
    private void storeComplete(String fileName) throws IOException {
        Socket client = filenameSockets.get(fileName);
        PrintWriter out = new PrintWriter(client.getOutputStream(), true);
        System.out.println();
        out.println(Protocol.STORE_COMPLETE_TOKEN);
    }
    /** Handles everything, whether controller receives LOAD or RELOAD **/
    private void loadOrReload(String fileName, Socket client, boolean isReload) throws IOException {
        PrintWriter out = new PrintWriter(client.getOutputStream(), true);
        Integer chosenPort = null;

        Iterator<Map.Entry<Integer, ConcurrentLinkedQueue<String>>> it = portFiles.entrySet().iterator();
        while(it.hasNext() && chosenPort == null){
            Map.Entry<Integer, ConcurrentLinkedQueue<String>> entry = it.next();
            if (entry.getValue().contains(fileName) && index.get(fileName) != null && index.get(fileName).equals("store complete")
                    && (!isReload || (filenamePort.get(fileName) != null
                    && !filenamePort.get(fileName).contains(entry.getKey())))) {
                chosenPort = entry.getKey();
            }
        }
        if (chosenPort == null) {
            out.println(Protocol.ERROR_LOAD_TOKEN);
            return;
        }
        out.println(Protocol.LOAD_FROM_TOKEN + " " + chosenPort + " " + filenameFilesize.get(fileName));
        if (isReload) {
            filenamePort.get(fileName).add(chosenPort);
        } else {
            filenamePort.remove(fileName);
            filenamePort.put(fileName, new ConcurrentLinkedQueue<>());
            filenamePort.get(fileName).add(chosenPort);
        }
    }
    /** Sends LIST with list of filenames that have been stored **/
    private void list(Socket client) throws IOException {
        Set<String> fileList = new HashSet<>();
        for (Map.Entry<String,String> entry : index.entrySet()) {
            if (entry.getValue().equals("store complete")) {
                fileList.add(entry.getKey());
            }
        }

        String list = String.join(", ", fileList);
        PrintWriter out = new PrintWriter(client.getOutputStream(), true);
        out.println(Protocol.LIST_TOKEN + " " + list);
    }
}