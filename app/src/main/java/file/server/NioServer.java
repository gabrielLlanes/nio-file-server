package file.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class NioServer {

    private static final Logger LOG = Logger.getLogger(NioServer.class.getName());

    private static ExecutorService pool;

    private static final Path BASE_PATH = Path.of("./nioTextFiles/");

    private static final Set<OpenOption> OPEN_NEW = new HashSet<>();

    private static final Set<OpenOption> OPEN_EXISITNG = new HashSet<>();

    /* do not use if expecting connections to drop during program runtime. */
    private static final boolean DELETE_ON_CLOSE = false;

    private static final InetSocketAddress SERVER_ADDRESS = new InetSocketAddress("0.0.0.0", 8080);

    private static Selector sel = null;

    private static long SELECT_TIMEOUT = 10_000;

    private static long startTime;

    static int MAX_CONNECTIONS = 1;

    private static int DEFAULT_BUFFER_SIZE = 8192;

    private static final int UUID_BYTE_ARRAY_LENGTH = 36;

    static {
        OPEN_NEW.add(StandardOpenOption.WRITE);
        OPEN_NEW.add(StandardOpenOption.TRUNCATE_EXISTING);
        OPEN_EXISITNG.add(StandardOpenOption.WRITE);
        OPEN_EXISITNG.add(StandardOpenOption.APPEND);
        if (DELETE_ON_CLOSE) {
            OPEN_NEW.add(StandardOpenOption.DELETE_ON_CLOSE);
        }
        try {
            Files.createDirectories(BASE_PATH);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static Process client;

    public static void main(String[] args) {
        pool = Executors.newFixedThreadPool(8);

        try {
            Files.createDirectories(BASE_PATH);
        } catch (IOException e1) {
            e1.printStackTrace();
        }

        List<String> clientArgs = null;

        if (args.length == 1) {
            MAX_CONNECTIONS = Integer.parseInt(args[0]);

            // clientArgs = new ArrayList<>();
            // clientArgs.add("java");
            // clientArgs.add("client.ClientExecutor");
            // clientArgs.add(args[0]);
        }
        Thread runServer = new Thread(() -> {
            try {
                runServer();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        });
        runServer.setDaemon(false);
        runServer.start();

        serverRunningLock.lock();
        while (!_serverRunning) {
            try {
                serverRunning.await();
            } catch (InterruptedException e) {
                continue;
            }
        }
        serverRunningLock.unlock();

        LOG.info(String.format("Server now running with %d max connections and args %s.\n",
                MAX_CONNECTIONS, clientArgs));

        try {
            client = clientArgs == null ? null
                    : Runtime.getRuntime().exec(clientArgs.toArray(new String[clientArgs.size()]));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        startTime = System.currentTimeMillis();
        try {
            runServer.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }

    private static ServerSocketChannel listener = null;

    private static SelectionKey listenerKey = null;

    private static final ReentrantLock serverRunningLock = new ReentrantLock();

    private static final Condition serverRunning = serverRunningLock.newCondition();

    private static boolean _serverRunning = false;

    private static void runServer() {
        try {
            sel = Selector.open();
        } catch (IOException e) {
            LOG.severe("COULD NOT OPEN THE SELECTOR");
            System.exit(1);
        }
        while (!_serverRunning) {
            try {
                openServer();
            } catch (IOException e) {
                listener = null;
                LOG.warning("Server establishment failed. Possible binding error.\n");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e1) {
                }
                continue;
            }
        }
        startSelecting();
        try {
            logStats();
        } catch (Exception e) {
            LOG.warning("Error logging metrics. Final data may not have been valid for output formatting.\n");
        }
        if (client != null) {
            int clientExitStatus = 0;
            try {
                clientExitStatus = client.waitFor();
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(1);
            }
            LOG.info(String.format("Client exited with status %d.\n", clientExitStatus));
        }
    }

    private static void openServer() throws IOException {
        serverRunningLock.lock();
        listener = ServerSocketChannel.open();
        listener.configureBlocking(false);
        listener.bind(SERVER_ADDRESS, MAX_CONNECTIONS);
        listenerKey = listener.register(sel, SelectionKey.OP_ACCEPT);
        // keyInUse.put(listenKey, false);
        _serverRunning = true;
        serverRunning.signal();
        serverRunningLock.unlock();
    }

    /* Variables related to selection key handling and thread pool management */

    private static AtomicInteger poolQueueSize = new AtomicInteger(0);

    private static int instantPoolSize = 0;

    private static int maxPoolSizeAttained = 0;

    private static int totalReadSelections = 0;

    private static int readPoolSkips = 0;

    private static ConcurrentMap<String, Status> connectionStatuses = new ConcurrentHashMap<>();

    private static ConcurrentMap<String, SelectionKey> connectionCurrentKeyMap = new ConcurrentHashMap<>();

    private static ConcurrentMap<SelectionKey, Boolean> keyInUse = new ConcurrentHashMap<>();

    private static int currClientNo = 1;

    private static Set<ClientConnectionAttachment> completedAttachments = new HashSet<>();

    private static AtomicInteger connectionsCompleted = new AtomicInteger(0);

    private static ReentrantLock completionLock = new ReentrantLock();

    private static Condition allConnectionsCompleted = completionLock.newCondition();

    private static boolean _allConnectionsCompleted = false;

    private static void startSelecting() {
        LOG.info("Selecting with " + SELECT_TIMEOUT + " ms timeout.\n");
        while (connectionsCompleted.get() < MAX_CONNECTIONS) {
            try {
                sel.select(SELECT_TIMEOUT);
            } catch (IOException ex) {
                LOG.warning("Problem occurred during selection.");
                continue;
            }
            if (sel.selectedKeys().size() == 0) {
                LOG.info("Selected key set size was 0");
                Set<SelectionKey> keys = sel.keys();
                closeDormantKeys(keys);
            }
            Iterator<SelectionKey> it = sel.selectedKeys().iterator();
            SelectionKey currKey = null;
            while (it.hasNext()) {
                try {
                    currKey = it.next();
                    handleKey2(currKey);
                } catch (CancelledKeyException e) {
                    LOG.info(String.format("Key with attachment %s was invalid.\n",
                            currKey.attachment()));
                }
                it.remove();
            }
        }
        serverFinish();
    }

    private static void closeDormantKeys(Set<SelectionKey> keys) {
        // LOG.info("Dormant keys size: " + keys.size());
        for (SelectionKey key : keys) {
            Boolean kInUse = keyInUse.get(key);
            if (key == listenerKey || (kInUse != null && kInUse))
                continue;
            Object attachment = key.attachment();
            if (attachment instanceof RegisterAttachment) {
                RegisterAttachment att = (RegisterAttachment) attachment;
                if (System.currentTimeMillis() - att.lastOperationMillis >= SELECT_TIMEOUT) {
                    closeConnectionAndCancel(key);
                    keyInUse.remove(key);
                }
            } else if (attachment instanceof ClientConnectionAttachment) {
                ClientConnectionAttachment att = (ClientConnectionAttachment) attachment;
                if (System.currentTimeMillis() - att.lastOperationMillis >= SELECT_TIMEOUT) {
                    LOG.warning(
                            String.format("Connection %s has read no bytes in 20 seconds. Closing connection now.\n",
                                    att.connectionID));
                    closeConnectionAndCancel(key);
                    try {
                        flushAndClose((ClientConnectionAttachment) attachment);
                    } catch (NullPointerException ex) {
                        System.out.println("Null pointer exception occurred: " + att);
                    } catch (IOException ex) {
                        LOG.severe(
                                String.format(
                                        "Error occurred during disk write for connection %s."
                                                + " Integrity of data cannot be guaranteed.\n",
                                        att.connectionID));
                    }
                    connectionStatuses.put(att.connectionID, Status.CLOSED);
                    keyInUse.remove(key);
                }
            }
            // keyInUse.remove(key);
        }
    }

    private static void handleKey(SelectionKey key) {
        // LOG.info(String.format("Selection made. key is %s\n. attachment is %s", key,
        // key.attachment()));
        if (key.channel() == listener) {
            finishConnect(key);
        } else {
            totalReadSelections++;
            Boolean b = keyInUse.get(key);
            if (b == null || (b != null && b)) {
                readPoolSkips++;
            } else {
                if (key.isAcceptable() || key.isConnectable()) {
                    System.out.println("Connection status changing.");
                }
                keyInUse.put(key, true);
                if ((instantPoolSize = poolQueueSize.incrementAndGet()) > maxPoolSizeAttained)
                    maxPoolSizeAttained = instantPoolSize;
                if (key.attachment() instanceof ClientConnectionAttachment) {
                    pool.execute(() -> dataRead(key));
                } else if (key.attachment() instanceof RegisterAttachment) {
                    pool.execute(() -> registerClient(key));
                } else {
                    LOG.warning("KEY NOT HANDLED CORRECTLY\n");
                }
            }
        }
    }

    private static void handleKey2(SelectionKey key) {
        // LOG.info(String.format("Selection made. key is %s\n. attachment is %s", key,
        // key.attachment()));
        if (key.channel() == listener) {
            finishConnect(key);
        } else {
            totalReadSelections++;
            Boolean b = keyInUse.get(key);
            if (b == null || (b != null && b)) {
                readPoolSkips++;
            } else {
                keyInUse.put(key, true);
                demuxKey(key);
            }
        }
    }

    private static void demuxKey(SelectionKey key) {
        if ((instantPoolSize = poolQueueSize.incrementAndGet()) > maxPoolSizeAttained)
            maxPoolSizeAttained = instantPoolSize;
        if (key.attachment() instanceof ClientConnectionAttachment) {
            pool.execute(() -> dataRead(key));
        } else if (key.attachment() instanceof RegisterAttachment) {
            pool.execute(() -> registerClient(key));
        } else {
            LOG.warning("KEY NOT HANDLED CORRECTLY");
        }
    }

    private static void finishConnect(SelectionKey key) {
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        checkListenerChannel(server);
        SocketChannel connection = null;
        //
        // Now that equality against the listener is verified, proceed using the
        // listener reference.
        //
        try {
            connection = listener.accept();
            if (connection == null) {
                LOG.warning(String.format(
                        "Connection attempt %d: At the time of accept, there may have been no pending connections.",
                        currClientNo));
                return;
            }
            connection.finishConnect();
            connection.configureBlocking(false);
        } catch (IOException e) {
            // connection attempt failed, so we drop the connection and do not modify the
            // state of the program
            LOG.warning("Initial connection attempt failed.\n");
            try {
                connection.close();
            } catch (IOException e1) {
            }
            return;
        }
        SelectionKey readKey = null;
        RegisterAttachment newRegisterAttachment = new RegisterAttachment();
        newRegisterAttachment.lastOperationMillis = System.currentTimeMillis();
        try {
            readKey = connection.register(sel, SelectionKey.OP_READ, newRegisterAttachment);
        } catch (ClosedChannelException ex) {
            LOG.severe("Listener should not be closed.\n");
            System.exit(1);
        }
        keyInUse.put(readKey, false);
        if (currClientNo > 0 && currClientNo % 10 == 0)
            LOG.info(String.format("10 more clients registered.\n%d connections made.\n",
                    currClientNo));
        checkConnectionCount();
        currClientNo++;
    }

    private static void registerClient(SelectionKey key) {
        // if (!key.isValid()) {
        // poolQueueSize.decrementAndGet();
        // keyInUse.remove(key);
        // return;
        // }
        // SocketChannel connection = (SocketChannel) key.channel();
        RegisterAttachment attachment = getRegisterAttachment(key);
        attachment.lastOperationMillis = System.currentTimeMillis();
        if (attachment.status == RegisterStatus.READ_ID) {
            try {
                readID(key);
            } catch (IOException ex) {
                LOG.warning(String.format("Connection was made, but establishment of connection ID failed.\n"));
                closeConnectionAndCancel(key);
            }

        } else {
            if (attachment.status == RegisterStatus.READ_FILE_SIZE) {
                try {
                    readFileSize(key);
                } catch (IOException ex) {
                    closeConnectionAndCancel(key);
                }
            }
            if (attachment.status == RegisterStatus.WRITE_POSITION) {
                try {
                    writePosition(key);
                } catch (IOException ex) {
                    LOG.warning(String
                            .format("Connection was made, but error occurred writing the amount of bytes read.\n"));
                    closeConnectionAndCancel(key);
                }
            } else if (attachment.status == RegisterStatus.VERIFY_POSITION) {
                try {
                    verifyPosition(key);
                } catch (IOException ex) {
                    LOG.warning(String.format("Failed to verify position: %s", ex.getMessage()));
                    closeConnectionAndCancel(key);
                }
            } else if (attachment.status == RegisterStatus.FINALIZE_NEW) {
                try {
                    finalizeNew(key);
                } catch (IOException ex) {
                    LOG.warning(String.format("Failure to finalize the new connection: %s", ex.getMessage()));
                    closeConnectionAndCancel(key);
                }
            } else if (attachment.status == RegisterStatus.FINALIZE_RESTABLISH) {
                ClientConnectionAttachment existing = getClientAttachment(
                        connectionCurrentKeyMap.get(attachment.connectionID));
                try {
                    finalizeReestablish(key, existing);
                } catch (IOException ex) {
                    LOG.warning(String.format("Failed to open existing file for id %s", attachment.connectionID));
                    closeConnectionAndCancel(key);
                }
            }

        }
        poolQueueSize.decrementAndGet();
        keyInUse.put(key, false);
    }

    public static boolean readID(SelectionKey key) throws IOException {
        RegisterAttachment att = getRegisterAttachment(key);
        SocketChannel ch = (SocketChannel) key.channel();
        if (att.idBuffer.hasRemaining()) {
            ch.read(att.idBuffer);
            att.connectionIDReads += 1;
        }
        if (!att.idBuffer.hasRemaining()) {
            byte[] idBytes = Arrays.copyOf(att.idBuffer.array(), UUID_BYTE_ARRAY_LENGTH);
            att.idBuffer.clear();
            att.connectionID = new String(idBytes);
            SelectionKey existingKey = null;
            if ((existingKey = connectionCurrentKeyMap.get(att.connectionID)) != null) {
                LOG.info(String.format("Read a connection id of existing connection %s.\n",
                        att.connectionID));
                ClientConnectionAttachment existingAttachment = getClientAttachment(existingKey);
                att.isReestablish = true;
                att.position = existingAttachment.totalBytesRead;
                att.posBuffer.putLong(existingAttachment.totalBytesRead);
                att.posBuffer.flip();
                if (connectionStatuses.get(att.connectionID) == Status.DATA_TRANSFER) {
                    LOG.warning(String.format(
                            "There seems to already be an open connection for ID %s. New connection will take precedence."
                                    + "Attachment status of existing connection is %s\n",
                            att.connectionID,
                            connectionCurrentKeyMap.get(att.connectionID).attachment()));
                    try {
                        closeConnectionAndCancel(existingKey);
                        flushAndClose(getClientAttachment(existingKey));
                    } catch (IOException ex) {
                    }
                }
            } else {
                att.isReestablish = false;
                att.position = 0;
                att.posBuffer.putLong(0);
                att.posBuffer.flip();
            }
            // key.interestOps(SelectionKey.OP_WRITE);
            // att.status = RegisterStatus.WRITE_POSITION;
            att.status = RegisterStatus.READ_FILE_SIZE;
            // writePosition(key);
            return true;
        } else
            return false;
    }

    private static void readFileSize(SelectionKey key) throws IOException {
        RegisterAttachment att = getRegisterAttachment(key);
        SocketChannel ch = (SocketChannel) key.channel();
        if (att.sizeBuffer.hasRemaining()) {
            ch.read(att.sizeBuffer);
        }
        if (!att.sizeBuffer.hasRemaining()) {
            att.sizeBuffer.flip();
            att.fileSize = att.sizeBuffer.getLong();
            if (att.isReestablish) {
                if (att.fileSize != getClientAttachment(
                        connectionCurrentKeyMap.get(att.connectionID)).fileSize) {
                    att.isReestablish = false;
                }
            }
            writePosition(key);
        } else {

        }
    }

    private static void finalizeNew(SelectionKey key) throws IOException {
        RegisterAttachment att = getRegisterAttachment(key);
        Path path = Path.of(
                BASE_PATH.toString(),
                String.format("connection-file-%s.txt", att.connectionID));
        if (Files.exists(path)) {
            LOG.info(String.format("Skipping file creation for %s. File will be overwritten.\n", att.connectionID));
        } else {
            try {
                Files.createFile(path);
            } catch (FileAlreadyExistsException ex) {
                // should not reach here
                LOG.info("Skipping file creation. File will be overwritten");
            } catch (IOException ex) {
                throw new IOException(
                        String.format("File creation failed for ID %s. Aborting connection.", att.connectionID), ex);
            }
        }
        FileChannel newOut = null;
        try {
            newOut = (FileChannel) Files.newByteChannel(path, OPEN_NEW);
        } catch (IOException ex) {
            throw new IOException(String.format(
                    "Error occured during file open of newly created file: %s. Aborting connection.\n",
                    att.connectionID), ex);
        }
        ClientConnectionAttachment newAttachment = new ClientConnectionAttachment();
        newAttachment.lastOperationMillis = System.currentTimeMillis();
        newAttachment.buffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        newAttachment.fileSize = att.fileSize;
        newAttachment.connectionID = att.connectionID;
        newAttachment.connectionIDReads = att.connectionIDReads;
        newAttachment.out = newOut;
        key.attach(newAttachment);
        connectionCurrentKeyMap.put(att.connectionID, key);
        connectionStatuses.put(att.connectionID, Status.DATA_TRANSFER);
        LOG.info(String.format("Connection %s has been newly established.\n", att.connectionID));
    }

    public static void writePosition(SelectionKey key) throws IOException {
        RegisterAttachment att = getRegisterAttachment(key);
        SocketChannel ch = (SocketChannel) key.channel();
        if (att.posBuffer.hasRemaining()) {
            ch.write(att.posBuffer);
        }
        if (!att.posBuffer.hasRemaining()) {
            att.posBuffer.clear();
            key.interestOps(SelectionKey.OP_READ);
            att.status = RegisterStatus.VERIFY_POSITION;
        }
    }

    public static void verifyPosition(SelectionKey key) throws IOException {
        RegisterAttachment att = getRegisterAttachment(key);
        SocketChannel ch = (SocketChannel) key.channel();
        if (att.posBuffer.hasRemaining()) {
            ch.read(att.posBuffer);
        }
        if (!att.posBuffer.hasRemaining()) {
            att.posBuffer.flip();
            if (att.posBuffer.getLong() != att.position) {
                throw new IOException("Position failed to agree.");
            }
            if (att.isReestablish) {
                finalizeReestablish(key, getClientAttachment(
                        connectionCurrentKeyMap.get(att.connectionID)));
            } else {
                finalizeNew(key);
            }
            // att.status = att.isReestablish ? RegisterStatus.FINALIZE_RESTABLISH :
            // RegisterStatus.FINALIZE_NEW;
        }
    }

    private static void finalizeReestablish(SelectionKey key,
            ClientConnectionAttachment existing) throws IOException {
        RegisterAttachment att = getRegisterAttachment(key);
        Path path = Path.of(
                BASE_PATH.toString(),
                String.format("connection-file-%s.txt", att.connectionID));
        FileChannel existingFileOut = (FileChannel) Files.newByteChannel(path, OPEN_EXISITNG);

        existing.out = existingFileOut;
        existing.connectionIDReads += att.connectionIDReads;
        key.attach(existing);
        connectionCurrentKeyMap.put(att.connectionID, key);
        connectionStatuses.put(att.connectionID, Status.DATA_TRANSFER);
        LOG.info(String.format(
                "Connection %s has been reestablished.\n"
                        + "Restored attachment state is %s\n",
                att.connectionID, existing));
    }

    private static void checkListenerChannel(ServerSocketChannel channel) {
        if (channel != listener) {
            LOG.severe("Referential equality for listening channels failed");
            System.exit(1);
        }
    }

    private static void checkConnectionCount() {
        if (currClientNo == MAX_CONNECTIONS) {
            LOG.info(
                    "SERVER: MAXIMUM CONNECTIONS REACHED\n");
        }
    }

    private static void dataRead(SelectionKey key) {
        // if (!key.isValid()) {
        // poolQueueSize.decrementAndGet();
        // keyInUse.remove(key);
        // return;
        // }
        ClientConnectionAttachment attachment = getClientAttachment(key);
        attachment.lastOperationMillis = System.currentTimeMillis();
        SocketChannel connection = null;
        connection = (SocketChannel) key.channel();
        long bytesRead = -2;
        try {
            // connection.write(ByteBuffer.allocate(1).put((byte) 0).flip());
            bytesRead = connection.read(attachment.buffer);
        } catch (IOException ex) {
            LOG.warning(String.format("Read failed. Connection needs to be reestablished."
                    + "Attachment status was %s. Error message: {%s}\n", attachment, ex.getMessage()));
            closeConnectionAndCancel(key);
            try {
                flushAndClose(attachment);
                attachment.out = null;
            } catch (IOException e) {
                LOG.severe(
                        String.format(
                                "Error occurred during disk write for connection %s."
                                        + " Integrity of data cannot be guaranteed.\n",
                                attachment.connectionID));
            } catch (NullPointerException e) {
                LOG.warning(String.format("Null pointer exception for connection %s.", attachment.showReferences()));
            }
            connectionStatuses.put(attachment.connectionID, Status.CLOSED);
            poolQueueSize.decrementAndGet();
            keyInUse.remove(key);
            return;
        }
        // System.out.print("Successful read and write. ");
        if (bytesRead == -1) {
            closeConnectionAndCancel(key);
            try {
                flushAndClose(attachment);
            } catch (IOException ex) {
                LOG.severe(String.format(
                        "Error occurred during disk write for connection %s. Integrity of data cannot be ensured even though EOF was read.",
                        attachment.connectionID));
            }
            LOG.info(String.format("Connection %s has been closed. Attachment status %s\n",
                    attachment.connectionID, attachment));
            connectionStatuses.remove(attachment.connectionID);
            connectionCurrentKeyMap.remove(attachment.connectionID);
            completedAttachments.add(attachment);
            attachment.buffer = null;
            checkCompletedCount();
            keyInUse.remove(key);
        } else {
            attachment.reads += 1;
            attachment.totalBytesRead += bytesRead;
            // if (attachment.totalBytesRead == attachment.fileSize) {
            // LOG.info(String.format("Connection %s: All bytes read.\n",
            // attachment.connectionID));
            // }
            // if (attachment.totalBytesRead == attachment.fileSize) {
            // key.interestOpsOr(SelectionKey.OP_WRITE);
            // LOG.info(String.format("Connection %s: All bytes read. Now sending 1 byte.",
            // attachment.connectionID));
            // try {
            // while (connection.write(ByteBuffer.allocate(1).put((byte) 1).flip()) == 0) {
            // try {
            // Thread.sleep(1);
            // } catch (InterruptedException e) {
            // }
            // }
            // // connection.write(ByteBuffer.allocate(1).put((byte) 1).flip());
            // } catch (IOException ex) {
            // }
            // }
            try {
                clearIfFull(attachment);
            } catch (IOException ex) {
                LOG.severe(String.format("Error occurred during disk write for connection %s. Aborting connection.\n",
                        attachment.connectionID));
                closeConnectionAndCancel(key);
                // connectionCurrentKeyMap.remove(attachment.connectionID);
                connectionStatuses.put(attachment.connectionID, Status.CLOSED);
                poolQueueSize.decrementAndGet();
                keyInUse.remove(key);
                return;
            } catch (NullPointerException ex) {
                System.out.println(attachment.buffer);
            }
            if (attachment.reads % 2000 == 0) {
                LOG.info(String.format("Connection %s: Read #%d read %d bytes. Buffer now at position %d/%d." +
                        " Total of %d KB read.\n", attachment.connectionID, attachment.reads,
                        bytesRead,
                        attachment.buffer.position(),
                        attachment.buffer.limit(), attachment.totalBytesRead / 1024));
            }
            poolQueueSize.decrementAndGet();
            keyInUse.put(key, false);
            // LOG.info("Successful read.\n");
        }
    }

    private static void closeConnectionAndCancel(SelectionKey key) {
        SocketChannel connection = (SocketChannel) key.channel();
        try {
            connection.close();
        } catch (IOException ex) {
        }
        key.cancel();
    }

    private static void clearIfFull(ClientConnectionAttachment attachment) throws IOException {
        if (!attachment.buffer.hasRemaining()) {
            flushToChannel(attachment.buffer, attachment.out);
            attachment.bufferFlushes = attachment.bufferFlushes + 1;
        }
    }

    private static void flushToChannel(ByteBuffer buffer, FileChannel out) throws IOException {
        buffer.flip();
        while (buffer.hasRemaining()) {
            out.write(buffer);
        }
        buffer.clear();
    }

    private static void flushAndClose(ClientConnectionAttachment attachment) throws IOException {
        flushToChannel(attachment.buffer, attachment.out);
        attachment.out.close();
    }

    private static void checkCompletedCount() {
        int n = -1;
        if ((n = connectionsCompleted.incrementAndGet()) == MAX_CONNECTIONS) {
            LOG.info("All connections " + connectionsCompleted.get()
                    + " completed. Now signaling and closing listener.\n");
            try {
                listener.close();
            } catch (IOException ex) {
                LOG.warning("Failed to close the listener.\n");
            }
            completionLock.lock();
            _allConnectionsCompleted = true;
            allConnectionsCompleted.signal();
            sel.wakeup();
            completionLock.unlock();
        }
        LOG.info("There are " + n + " completed connections now.\n");
    }

    private static void serverFinish() {
        LOG.info("Entering serverFinish()");
        pool.shutdown();
        completionLock.lock();
        while (!_allConnectionsCompleted) {
            try {
                allConnectionsCompleted.await();
            } catch (InterruptedException e) {
                continue;
            }
        }
        completionLock.unlock();
        try {
            if (pool.awaitTermination(10, TimeUnit.MILLISECONDS))
                LOG.info("READ POOL SHUTDOWN SUCCESSFULLY\n");
        } catch (InterruptedException e) {
        }
    }

    private static void logStats() {
        Stream<ClientConnectionAttachment> s = completedAttachments.stream();
        Stream<ClientConnectionAttachment> s2 = completedAttachments.stream();
        long totalBytesRead = s.reduce(0L, (sum, attachment) -> sum + attachment.totalBytesRead, (n, m) -> n + m);
        long totalReads = s2.reduce(0L, (sum, attachment) -> sum + attachment.reads, (n, m) -> n + m);

        double elapsedTime = (System.currentTimeMillis() - startTime) / 1000.0f;
        long totalKBRead = totalBytesRead / 1024;
        long averageKBPerRead = totalKBRead / totalReads;
        long averageBytesPerRead = totalBytesRead / totalReads;
        long averageKBs = (long) (totalKBRead / elapsedTime);

        LOG.info(String.format(
                "All %d connections completed.\n%s total bytes read.\n%s total KB read.\n%s total reads made.\n"
                        + "Average of %d bytes/%d KB per read.\nAverage of %s KB/s.\nTotal elapsed time %fs.\n"
                        + "%d total read selections. %d total readPool skips. Maximum readPool queue size was %d.\n",
                // completedAttachments,
                connectionsCompleted.get(),
                NumberFormat.getNumberInstance(Locale.US).format(totalBytesRead),
                NumberFormat.getNumberInstance(Locale.US).format(totalKBRead),
                NumberFormat.getNumberInstance(Locale.US).format(totalReads),
                averageBytesPerRead,
                averageKBPerRead,
                NumberFormat.getNumberInstance(Locale.US).format(averageKBs),
                elapsedTime, totalReadSelections, readPoolSkips, maxPoolSizeAttained));
    }

    enum Status {
        DATA_TRANSFER,
        CLOSED
    }

    private static class ClientConnectionAttachment {
        long connectionIDReads = 0;
        String connectionID = null;
        long fileSize = 0;
        FileChannel out = null;
        ByteBuffer buffer = null;
        long reads = 0;
        long totalBytesRead = 0;
        long bufferFlushes = 0;
        long lastOperationMillis;

        ClientConnectionAttachment() {
        }

        @Override
        public String toString() {
            return String.format(
                    "{%s: %d reads, %d total KB read, %d total bytes read, %d connectionID reads}\n",
                    connectionID,
                    reads,
                    totalBytesRead / 1024,
                    totalBytesRead,
                    connectionIDReads);
        }

        public String showReferences() {
            return String.format("Buffer: {%s}, FileChannel: {%s}", buffer.toString(), out.toString());
        }
    }

    private static ClientConnectionAttachment getClientAttachment(SelectionKey key) {
        return (ClientConnectionAttachment) key.attachment();
    }

    enum RegisterStatus {
        READ_ID,
        READ_FILE_SIZE,
        WRITE_POSITION,
        VERIFY_POSITION,
        FINALIZE_NEW,
        FINALIZE_RESTABLISH
    }

    private static class RegisterAttachment {
        RegisterStatus status = RegisterStatus.READ_ID;
        ByteBuffer idBuffer = ByteBuffer.allocate(UUID_BYTE_ARRAY_LENGTH);
        long connectionIDReads = 0;
        String connectionID = null;
        ByteBuffer posBuffer = ByteBuffer.allocate(Long.BYTES);
        long position;
        ByteBuffer sizeBuffer = ByteBuffer.allocate(Long.BYTES);
        long fileSize;
        boolean isReestablish = false;
        long lastOperationMillis;
    }

    private static RegisterAttachment getRegisterAttachment(SelectionKey key) {
        return (RegisterAttachment) key.attachment();
    }
}
