import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;

public class Dstore {
    int port;
    int cport;
    int timeout;
    String file_folder;
    File folder;
    ConcurrentHashMap<String,Socket> typeSocket;

    public Dstore(int port, int cport, int timeout, String file_folder) {
        this.port = port;
        this.cport = cport;
        this.timeout = timeout;
        this.file_folder = file_folder;
        createFile(file_folder);
        typeSocket = new ConcurrentHashMap<>();
        try {
            Socket controllerSocket = new Socket(InetAddress.getLocalHost(), this.cport);
            typeSocket.put("Controller", controllerSocket);
            PrintWriter controllerOut = new PrintWriter(controllerSocket.getOutputStream(), true);
            controllerOut.println(Protocol.JOIN_TOKEN + " " + this.port);
            new Thread(new DstoreServiceThread(controllerSocket, "Controller")).start();
        } catch (IOException e) {
            e.printStackTrace();
        }
        initialise();
    }

    public static void main(String[] args) {
        Dstore dstore = new Dstore(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]), args[3]);
    }
    public void initialise() {
        try {
            ServerSocket ss = new ServerSocket(this.port);
            while (true) {
                try {
                    var client = ss.accept();
                    new Thread(new DstoreServiceThread(client, "Client")).start();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    class DstoreServiceThread implements Runnable {
        Socket clientSocket;
        String socketType;

        DstoreServiceThread(Socket c, String type) {
            this.clientSocket = c;
            this.socketType = type;
            typeSocket.put(type, clientSocket);
        }

        public void run() {
            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                String line;
                String[] arr;
                while ((line = in.readLine()) != null) {
                    System.out.println(line);
                    arr = line.split(" ");
                    if (socketType.equals("Controller")) {
                        /** If protocol is REMOVE **/
                        if (arr[0].equals(Protocol.REMOVE_TOKEN)) {
                            String filename = arr[1];
                            File file = new File(file_folder + "/" + filename);
                            file.delete();
                            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                            out.println(Protocol.REMOVE_ACK_TOKEN + " " + filename);
                        } else {
                        }
                    } else if (socketType.equals("Client")) {
                        /** If protocol is STORE **/
                        if (arr[0].equals(Protocol.STORE_TOKEN)) {
                            clientSocket.setSoTimeout(timeout);
                            String fileName = arr[1];
                            File fileToStore = new File(folder, fileName);
                            if (fileToStore.createNewFile()) {
                                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                                out.println(Protocol.ACK_TOKEN);
                                try (InputStream inStream = clientSocket.getInputStream();
                                     FileOutputStream outStream = new FileOutputStream(fileToStore)) {
                                    byte[] buf = new byte[1000];
                                    int buflen;
                                    while ((buflen = inStream.read(buf)) != -1) {
                                        outStream.write(buf, 0, buflen);
                                    }

                                    Socket controllerSocket = typeSocket.get("Controller");
                                    PrintWriter controllerOut = new PrintWriter(controllerSocket.getOutputStream(), true);
                                    controllerOut.println(Protocol.STORE_ACK_TOKEN + " " + fileName);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }

                            } else {
                                System.out.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                                PrintWriter out = new PrintWriter(clientSocket.getOutputStream());
                                out.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                                out.flush();
                            }
                        /** If protocol is LIST **/
                        } else if (arr[0].equals(Protocol.LIST_TOKEN)) {
                            folder.listFiles();
                        /** If protocol is LOAD **/
                        } else if (arr[0].equals(Protocol.LOAD_DATA_TOKEN)) {
                            String filename = arr[1];
                            String fullPath = file_folder + "/" + filename;
                            try {
                                Path path = Paths.get(fullPath);
                                byte[] fileContent = Files.readAllBytes(path);
                                OutputStream outputStream = clientSocket.getOutputStream();
                                outputStream.write(fileContent);
                                outputStream.flush();
                                clientSocket.setSoTimeout(timeout);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        } else {
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (socketType.equals("Client")) {
                    typeSocket.remove("Client");
                }
            }
        }
    }
    public void createFile(String folderPath) {
        this.folder = new File(folderPath);
        if (!this.folder.exists()) {
            this.folder.mkdir();
        } else {
            String[] entries = this.folder.list();
            int i = 0;
            while (i < entries.length) {
                String s = entries[i];
                File currentFile = new File(this.folder.getPath(), s);
                currentFile.delete();
                i++;
            }
            this.folder.delete();
            this.folder.mkdir();
        }
    }
}