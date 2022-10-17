package server;

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
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.stream.Stream;
import server.attachment.InitializationAttachment;
import server.attachment.DataTransferAttachment;
import server.status.RegistrationStatus;
import server.status.DataTransferStatus;
import server.openoptionchoices.OpenOptionChoices;

/*This is a simple file transfer program over TCP using non-blocking IO provided by the nio package. */
public class NioFileServerPreRefactor {

  private static final Logger log = Logger.getLogger(NioFileServerPreRefactor.class.getName());

  private static final ExecutorService pool = Executors.newFixedThreadPool(32);

  private static final Path BASE_PATH = Path.of("client-files");

  private static final Path LOG_PATH = Path.of(BASE_PATH.toString(), "log.txt");

  private static final int PORT = 11500;

  private static final InetSocketAddress SERVER_ADDRESS = new InetSocketAddress(System.getenv("NIO_FILE_SERVER_IP"),
      PORT);

  private static final int MAX_CONCURRENT_CONNECTIONS = Integer
      .parseInt(System.getenv("NIO_FILE_SERVER_MAXIMUM_CONNECTIONS"));

  private static Selector sel;

  private static final long SELECT_TIMEOUT = 10_000;

  private static long startTime;

  private static int DEFAULT_BUFFER_SIZE = 8192;

  public static final int UUID_BYTE_ARRAY_LENGTH = 36;

  public static void main(String[] args) throws SecurityException, IOException {
    initialize();
    runServer();
  }

  private static void initialize() {
    try {
      Files.createDirectories(BASE_PATH);
      FileHandler handler = new FileHandler(LOG_PATH.toString());
      handler.setFormatter(new SimpleFormatter());
      log.addHandler(handler);
      log.setLevel(Level.ALL);
      sel = Selector.open();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void runServer() {
    openListener();
    startSelecting();
  }

  private static ServerSocketChannel listener;

  private static SelectionKey listenerKey;

  private static ReentrantLock listenerKeyLock = new ReentrantLock();

  private static void openListener() {
    int openAttempts = 1;
    while (true) {
      try {
        listener = ServerSocketChannel.open();
        listener.configureBlocking(false);
        listener.bind(SERVER_ADDRESS);
        listenerKey = listener.register(sel, SelectionKey.OP_ACCEPT);
        startTime = System.currentTimeMillis();
        log.info(String.format("Server now running with %d maximum concurrent connections.\n",
            MAX_CONCURRENT_CONNECTIONS));
        break;

      } catch (IOException e) {
        log.log(Level.WARNING,
            String.format("There was an error opening the listening socket on attempt %d : %s.\n",
                openAttempts++, e.getMessage()));
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e1) {
        }
      }
    }
  }

  /* Variables related to selection key handling and thread pool management */

  private static AtomicInteger poolQueueSize = new AtomicInteger(0);

  private static int instantPoolSize = 0;

  private static int maxPoolSizeAttained = 0;

  private static int totalReadSelections = 0;

  private static int readPoolSkips = 0;

  private static AtomicInteger activeConnections = new AtomicInteger(0);

  private static ConcurrentMap<String, DataTransferStatus> connectionStatuses = new ConcurrentHashMap<>();

  private static ConcurrentMap<String, SelectionKey> connectionCurrentKeyMap = new ConcurrentHashMap<>();

  private static ConcurrentMap<SelectionKey, Boolean> keyInUse = new ConcurrentHashMap<>();

  private static ConcurrentMap<String, DataTransferAttachment> completedAttachments = new ConcurrentHashMap<>();

  private static AtomicInteger connectionsCompleted = new AtomicInteger(0);

  private static void startSelecting() {
    log.info("Selecting with " + SELECT_TIMEOUT + " ms timeout.\n");
    while (true) {
      try {
        sel.select(SELECT_TIMEOUT);
      } catch (IOException ex) {
        log.warning("Problem occurred during selection.\n");
        continue;
      }
      if (sel.selectedKeys().size() == 0) {
        log.info("Selected key set size was 0.\n");
      } else {
        Iterator<SelectionKey> it = sel.selectedKeys().iterator();
        SelectionKey currKey = null;
        while (it.hasNext()) {
          try {
            currKey = it.next();
            handleKey(currKey);
          } catch (CancelledKeyException e) {
          }
          it.remove();
        }
      }
    }
  }

  private static void handleKey(SelectionKey key) {
    int currentActiveConnections = activeConnections.get();
    if (key.channel() == listener && currentActiveConnections < MAX_CONCURRENT_CONNECTIONS) {
      finishConnect(key);
    } else if (key.channel() == listener && currentActiveConnections >= MAX_CONCURRENT_CONNECTIONS) {
      log.warning(
          String.format("Connection count is currently %d. No connection attempt will be accepted right now.\n",
              currentActiveConnections));
    } else {
      totalReadSelections++;
      boolean b;
      try {
        b = keyInUse.get(key);
      } catch (NullPointerException e) {
        return;
      }
      if (b) {
        return;
      } else {
        keyInUse.put(key, true);
        if ((instantPoolSize = poolQueueSize.incrementAndGet()) > maxPoolSizeAttained)
          maxPoolSizeAttained = instantPoolSize;
        if (key.attachment() instanceof DataTransferAttachment && key.interestOps() == SelectionKey.OP_READ) {
          pool.execute(() -> dataRead(key));
        } else if (key.attachment() instanceof DataTransferAttachment && key.interestOps() == SelectionKey.OP_WRITE) {
          pool.execute(() -> writeAndClose(key));
        } else if (key.attachment() instanceof InitializationAttachment) {
          pool.execute(() -> registerClient(key));
        } else {
          log.warning(String.format("Selection key %s is not in a valid state.\n", key.toString()));
        }
      }
    }
  }

  // @SuppressWarnings("null")
  private static void finishConnect(SelectionKey key) {
    SocketChannel connection = null;
    try {
      connection = listener.accept();
      if (connection == null) {
        log.warning(String.format("Listening socket tried to accept connection, but no connection was made.\n"));
        return;
      }
      connection.finishConnect();
      connection.configureBlocking(false);
    } catch (IOException e) {
      /*
       * connection attempt failed, so we drop the connection and do not modify the
       * state of the program
       */
      log.warning("Initial connection attempt failed.\n");
      closeConnectionAndCancel(key);
      return;
    }
    SelectionKey readKey = null;
    InitializationAttachment registrationAttachment = new InitializationAttachment();
    registrationAttachment.lastOperationMillis = System.currentTimeMillis();
    try {
      readKey = connection.register(sel, SelectionKey.OP_READ, registrationAttachment);
    } catch (ClosedChannelException ex) {
      log.log(Level.SEVERE, "Listener should not be closed.\n", ex);
      System.exit(1);
    }
    keyInUse.put(readKey, false);
    int instantActiveConnections = activeConnections.incrementAndGet();
    listenerKeyLock.lock();
    if (instantActiveConnections >= MAX_CONCURRENT_CONNECTIONS) {
      log.info("Maximum concurrent connections attained. Temporarily deregistering listener from selector.\n");
      listenerKey.cancel();
    }
    listenerKeyLock.unlock();
    if (instantActiveConnections % 10 == 0 && instantActiveConnections > 0) {
      log.info(String.format("There are %d active connections.\n", instantActiveConnections));
    }
  }

  private static void registerClient(SelectionKey key) {
    InitializationAttachment attachment = (InitializationAttachment) key.attachment();
    attachment.lastOperationMillis = System.currentTimeMillis();
    boolean errorOccurred = false;
    if (attachment.status == RegistrationStatus.READ_ID) {
      try {
        readID(key);
      } catch (IOException ex) {
        log.warning(String.format("Socket connection was made, but reading connection ID failed.\n"));
        closeConnectionAndCancel(key);
        errorOccurred = true;
      }

    } else if (attachment.status == RegistrationStatus.READ_FILE_SIZE) {
      try {
        readFileSize(key);
      } catch (IOException ex) {
        log.warning(String.format("Error occurred reading file size for connection %s.\n", attachment.connectionID));
        closeConnectionAndCancel(key);
        errorOccurred = true;
      }
    }
    if (attachment.status == RegistrationStatus.WRITE_POSITION) {
      try {
        writePosition(key);
      } catch (IOException ex) {
        log.warning(String
            .format("Error occurred writing the current amount of bytes read for connection %s.\n",
                attachment.connectionID));
        closeConnectionAndCancel(key);
        errorOccurred = true;
      }
    } else if (attachment.status == RegistrationStatus.VERIFY_POSITION) {
      try {
        verifyPosition(key);
      } catch (IOException ex) {
        log.warning(String.format("Error occurred verifying position for connection %s.\n", attachment.connectionID));
        closeConnectionAndCancel(key);
        errorOccurred = true;
      }
    }
    if (attachment.status == RegistrationStatus.FINALIZE_NEW) {
      try {
        finalizeNew(key);
      } catch (IOException ex) {
        log.warning(String.format("Failure to finalize the new connection %s.\n", attachment.connectionID));
        closeConnectionAndCancel(key);
        errorOccurred = true;
      }
    } else if (attachment.status == RegistrationStatus.FINALIZE_RESTABLISH) {
      DataTransferAttachment existing = (DataTransferAttachment) connectionCurrentKeyMap
          .get(attachment.connectionID).attachment();
      try {
        finalizeReestablish(key, existing);
      } catch (IOException ex) {
        log.warning(String.format("Failed to open existing file for connection %s/\n", attachment.connectionID));
        closeConnectionAndCancel(key);
        errorOccurred = true;
      }
    }
    poolQueueSize.decrementAndGet();
    if (errorOccurred)
      keyInUse.remove(key);
    else
      keyInUse.put(key, false);
  }

  public static void readID(SelectionKey key) throws IOException {
    InitializationAttachment att = (InitializationAttachment) key.attachment();
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
        log.info(String.format("Read a connection id of existing connection %s.\n",
            att.connectionID));
        DataTransferAttachment existingAttachment = (DataTransferAttachment) existingKey.attachment();
        att.isReestablish = true;
        att.position = existingAttachment.totalBytesRead;
        att.posBuffer.putLong(existingAttachment.totalBytesRead);
        att.posBuffer.flip();
        if (connectionStatuses.get(att.connectionID) == DataTransferStatus.DATA_TRANSFER) {
          log.warning(String.format(
              "There seems to already be an open connection for ID %s. New connection will take precedence."
                  + "Attachment status of existing connection is %s\n",
              att.connectionID, existingAttachment));
          closeConnectionAndCancel(existingKey);
          try {
            flushAndClose(((DataTransferAttachment) existingKey.attachment()));
          } catch (IOException ex) {
            log.warning(String.format("Failed flush for connection %s.\n", att.connectionID));
          }
        }
      } else {
        att.isReestablish = false;
        att.position = 0;
        att.posBuffer.putLong(0);
        att.posBuffer.flip();
      }
      att.status = RegistrationStatus.READ_FILE_SIZE;
    }
  }

  private static void readFileSize(SelectionKey key) throws IOException {
    InitializationAttachment att = (InitializationAttachment) key.attachment();
    SocketChannel ch = (SocketChannel) key.channel();
    if (att.sizeBuffer.hasRemaining()) {
      ch.read(att.sizeBuffer);
    }
    if (!att.sizeBuffer.hasRemaining()) {
      att.sizeBuffer.flip();
      att.fileSize = att.sizeBuffer.getLong();
      if (att.isReestablish) {
        DataTransferAttachment existing = (DataTransferAttachment) connectionCurrentKeyMap.get(att.connectionID)
            .attachment();
        if (att.fileSize != existing.fileSize) {
          att.isReestablish = false;
        }
      }
      att.status = RegistrationStatus.WRITE_POSITION;
      key.interestOps(SelectionKey.OP_WRITE);
    }
  }

  private static void finalizeNew(SelectionKey key) throws IOException {
    InitializationAttachment att = (InitializationAttachment) key.attachment();
    Path path = Path.of(
        BASE_PATH.toString(),
        String.format("connection-file-%s.txt", att.connectionID));
    FileChannel newOut = null;
    if (!Files.exists(path)) {
      try {
        newOut = (FileChannel) Files.newByteChannel(path, OpenOptionChoices.OPEN_NEW);
      } catch (IOException ex) {
        throw new IOException(String.format(
            "Error occurred during file open of newly created file: %s. Aborting connection.\n",
            att.connectionID), ex);
      }
    } else {
      log.info(String.format("Skipping file creation for %s. File will be overwritten.\n", att.connectionID));
      try {
        newOut = (FileChannel) Files.newByteChannel(path, OpenOptionChoices.TRUNCATE_EXISTING);
      } catch (IOException ex) {
        throw new IOException(String.format(
            "Error occurred during file open of existing created file: %s. Aborting connection.\n",
            att.connectionID), ex);
      }
    }
    DataTransferAttachment newAttachment = new DataTransferAttachment();
    newAttachment.lastOperationMillis = System.currentTimeMillis();
    newAttachment.buffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
    newAttachment.fileSize = att.fileSize;
    newAttachment.connectionID = att.connectionID;
    newAttachment.connectionIDReads = att.connectionIDReads;
    newAttachment.out = newOut;
    key.attach(newAttachment);
    connectionCurrentKeyMap.put(att.connectionID, key);
    connectionStatuses.put(att.connectionID, DataTransferStatus.DATA_TRANSFER);
    log.info(String.format("Connection %s has been newly established.\n", att.connectionID));
  }

  public static void writePosition(SelectionKey key) throws IOException {
    InitializationAttachment att = (InitializationAttachment) key.attachment();
    SocketChannel ch = (SocketChannel) key.channel();
    while (att.posBuffer.hasRemaining()) {
      ch.write(att.posBuffer);
    }
    if (!att.posBuffer.hasRemaining()) {
      att.posBuffer.clear();
      att.status = RegistrationStatus.VERIFY_POSITION;
      key.interestOps(SelectionKey.OP_READ);
    }
  }

  public static void verifyPosition(SelectionKey key) throws IOException {
    InitializationAttachment att = (InitializationAttachment) key.attachment();
    SocketChannel ch = (SocketChannel) key.channel();
    if (att.posBuffer.hasRemaining()) {
      ch.read(att.posBuffer);
    }
    if (!att.posBuffer.hasRemaining()) {
      att.posBuffer.flip();
      if (att.posBuffer.getLong() != att.position) {
        throw new IOException("Position failed to agree.");
      }
      att.status = att.isReestablish ? RegistrationStatus.FINALIZE_RESTABLISH : RegistrationStatus.FINALIZE_NEW;
    }
  }

  private static void finalizeReestablish(SelectionKey key,
      DataTransferAttachment existing) throws IOException {
    InitializationAttachment att = (InitializationAttachment) key.attachment();
    Path path = Path.of(
        BASE_PATH.toString(),
        String.format("connection-file-%s.txt", att.connectionID));
    FileChannel existingFileOut = (FileChannel) Files.newByteChannel(path, OpenOptionChoices.APPEND_EXISTING);
    existing.out = existingFileOut;
    existing.connectionIDReads += att.connectionIDReads;
    key.attach(existing);
    connectionCurrentKeyMap.put(att.connectionID, key);
    connectionStatuses.put(att.connectionID, DataTransferStatus.DATA_TRANSFER);
    if (existing.totalBytesRead == existing.fileSize) {
      writeAndClose(key);
    }
    log.info(String.format(
        "Connection %s has been reestablished.\n"
            + "Restored attachment state is %s\n",
        att.connectionID, existing));
  }

  private static void dataRead(SelectionKey key) {
    DataTransferAttachment attachment = (DataTransferAttachment) key.attachment();
    attachment.lastOperationMillis = System.currentTimeMillis();
    SocketChannel connection = null;
    connection = (SocketChannel) key.channel();
    long bytesRead;
    try {
      bytesRead = connection.read(attachment.buffer);
    } catch (IOException e) {
      log.log(Level.WARNING, String.format("Read failed. Connection needs to be reestablished."
          + "Attachment status was %s", attachment), e);
      System.out.printf("Data read failed for connection %s. Closing connection.\n", attachment.connectionID);
      closeConnectionAndCancel(key);
      try {
        flushAndClose(attachment);
        attachment.out = null;
      } catch (IOException e2) {
        log.severe(
            String.format(
                "Error occurred during disk write for connection %s."
                    + " Integrity of data cannot be guaranteed.\n",
                attachment.connectionID));
      } catch (NullPointerException e2) {
        log.warning(String.format("Null pointer exception for connection %s.\n", attachment.channelsString()));
      }
      connectionStatuses.put(attachment.connectionID, DataTransferStatus.CLOSED);
      poolQueueSize.decrementAndGet();
      keyInUse.remove(key);
      return;
    }
    if (bytesRead == -1) {
      closeConnectionAndCancel(key);
      try {
        flushAndClose(attachment);
      } catch (IOException e) {
        log.log(Level.SEVERE, String.format(
            "Error occurred during disk write for connection %s. Integrity of data cannot be ensured even though EOF was read.",
            attachment.connectionID), e);
      }
      log.info(String.format("Connection %s has been closed. Attachment status %s\n",
          attachment.connectionID, attachment));
      connectionStatuses.remove(attachment.connectionID);
      connectionCurrentKeyMap.remove(attachment.connectionID);
      completedAttachments.put(attachment.connectionID, attachment);
      if (completedAttachments.size() == MAX_CONCURRENT_CONNECTIONS) {
        logStats();
      }
      attachment.buffer = null;
      keyInUse.remove(key);
    } else {
      attachment.reads += 1;
      attachment.totalBytesRead += bytesRead;
      if (attachment.totalBytesRead == attachment.fileSize) {
        log.info(String.format("Connection %s: All bytes read.\n",
            attachment.connectionID));
        try {
          flushAndClose(attachment);
        } catch (IOException e) {
          log.log(Level.SEVERE, String.format(
              "Error occurred during disk write for connection %s. Integrity of data cannot be ensured.",
              attachment.connectionID), e);
        }
        // key.interestOps(SelectionKey.OP_WRITE);
        writeAndClose(key);
      } else {
        try {
          clearIfFull(attachment);
        } catch (IOException ex) {
          log.severe(String.format("Error occurred during disk write for connection %s. Aborting connection.\n",
              attachment.connectionID));
          closeConnectionAndCancel(key);
          connectionStatuses.put(attachment.connectionID, DataTransferStatus.CLOSED);
          poolQueueSize.decrementAndGet();
          keyInUse.remove(key);
          return;
        } catch (NullPointerException ex) {
          System.out.println("Null: " + attachment.buffer);
        }
      }
      if (attachment.reads % 2000 == 0) {
        log.info(String.format("Connection %s: Read #%d read %d bytes. Buffer now at position %d/%d." +
            " Total of %d KB read.\n", attachment.connectionID, attachment.reads,
            bytesRead,
            attachment.buffer.position(),
            attachment.buffer.limit(), attachment.totalBytesRead / 1024));
      }
      keyInUse.put(key, false);
    }
    poolQueueSize.decrementAndGet();
  }

  private static void writeAndClose(SelectionKey key) {
    SocketChannel connection = (SocketChannel) key.channel();
    DataTransferAttachment attachment = (DataTransferAttachment) key.attachment();
    if (attachment.out.isOpen()) {
      try {
        flushAndClose(attachment);
      } catch (IOException e) {
        log.log(Level.SEVERE, String.format(
            "Error occurred during disk write for connection %s. Integrity of data cannot be ensured.",
            attachment.connectionID), e);
      }
    }
    try {
      while (connection.write(ByteBuffer.wrap(new byte[] { -86 })) != 1) {
      }
    } catch (IOException e) {
      keyInUse.put(key, false);
      poolQueueSize.decrementAndGet();
      return;
    }
    closeConnectionAndCancel(key);
    connectionStatuses.remove(attachment.connectionID);
    connectionCurrentKeyMap.remove(attachment.connectionID);
    completedAttachments.put(attachment.connectionID, attachment);
    if (completedAttachments.size() == MAX_CONCURRENT_CONNECTIONS) {
      logStats();
    }
    attachment.buffer = null;
    poolQueueSize.decrementAndGet();
    keyInUse.remove(key);

  }

  private static void closeConnectionAndCancel(SelectionKey key) {
    if (key.attachment() instanceof DataTransferAttachment) {
      DataTransferAttachment att = (DataTransferAttachment) key.attachment();
      System.out.printf("Closing connection for %s. Read %d bytes in total.\n", att.connectionID, att.totalBytesRead);
    }
    SocketChannel connection = (SocketChannel) key.channel();
    try {
      connection.close();
    } catch (IOException ex) {
    }
    key.cancel();
    listenerKeyLock.lock();
    if (activeConnections.decrementAndGet() < MAX_CONCURRENT_CONNECTIONS && !listenerKey.isValid()) {
      log.info("Re-registering listener with selector.\n");
      try {
        listenerKey = listener.register(sel, SelectionKey.OP_ACCEPT);
      } catch (ClosedChannelException e) {
        log.severe("Selector should not be closed.\n");
        System.exit(1);
      }
    }
    listenerKeyLock.unlock();
  }

  private static void clearIfFull(DataTransferAttachment attachment) throws IOException {
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

  private static void flushAndClose(DataTransferAttachment attachment) throws IOException {
    flushToChannel(attachment.buffer, attachment.out);
    attachment.out.close();
  }

  private static void logStats() {
    Stream<Entry<String, DataTransferAttachment>> s = completedAttachments.entrySet().stream();
    Stream<Entry<String, DataTransferAttachment>> s2 = completedAttachments.entrySet().stream();
    long totalBytesRead = s.reduce(0L, (sum, entry) -> sum + entry.getValue().totalBytesRead, (n, m) -> n + m);
    long totalReads = s2.reduce(0L, (sum, entry) -> sum + entry.getValue().reads, (n, m) -> n + m);

    double elapsedTime = (System.currentTimeMillis() - startTime) / 1000.0f;
    long totalKBRead = totalBytesRead / 1024;
    long averageKBPerRead = totalKBRead / totalReads;
    long averageBytesPerRead = totalBytesRead / totalReads;
    long averageKBs = (long) (totalKBRead / elapsedTime);
    String logString = String.format(
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
        elapsedTime, totalReadSelections, readPoolSkips, maxPoolSizeAttained);
    log.info(logString);
    System.out.println(logString);
  }
}
