package server.connection;

import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import server.attachment.DataTransferAttachment;
import server.attachment.UploadRequestAttachment;
import server.attachment.UploadRequestAttachment.UploadRequestDetails;
import server.channelioutil.ChannelIOUtil;
import server.channelioutil.FileIOUtil;
import server.channelmultiplexor.ConnectionKeyMultiplexor;
import server.openoptionchoices.OpenOptionChoices;
import server.status.DataTransferStatus;

public class ConnectionManager {

  private static final ConnectionManager instance = new ConnectionManager();

  private static final String basePath = "client-files";

  private static final Logger log = System.getLogger(ConnectionManager.class.getName());

  private ConnectionKeyMultiplexor initializationMultiplexor;

  private ConnectionKeyMultiplexor uploadMultiplexor;

  private int maxActiveConnections = 10_000;

  private AtomicInteger activeConnections = new AtomicInteger(0);

  private final ConcurrentMap<SelectionKey, Semaphore> uploadRequestKeySemaphoreMap = new ConcurrentHashMap<>();

  private final ConcurrentMap<String, DataTransferStatus> dataTransferStatusMap = new ConcurrentHashMap<>();

  private final ConcurrentMap<String, Semaphore> idFileNameSemaphoreMap = new ConcurrentHashMap<>();

  private final ConcurrentMap<String, SelectionKey> dataTransferCurrentKeyMap = new ConcurrentHashMap<>();

  static {
    Path basePathDir = Path.of(basePath);
    if (!Files.exists(basePathDir)) {
      try {
        Files.createDirectories(Path.of(basePath));
      } catch (IOException e) {
        log.log(Level.ERROR, "Could not create directory for which to store files.", e);
        System.exit(1);
      }
    }
  }

  private ConnectionManager() {
  }

  public static ConnectionManager getInstance() {
    return instance;
  }

  public void setMaxConnections(int n) {
    maxActiveConnections = n;
  }

  public void setInitializationMultiplexor(ConnectionKeyMultiplexor initializationMultiplexor) {
    this.initializationMultiplexor = initializationMultiplexor;
  }

  public void setUploadMultiplexor(ConnectionKeyMultiplexor uploadMultiplexor) {
    this.uploadMultiplexor = uploadMultiplexor;
  }

  public boolean tryAcquire(SelectionKey key) {
    if (key.attachment() instanceof UploadRequestAttachment) {
      Semaphore sem = uploadRequestKeySemaphoreMap.get(key);
      return sem == null ? false : sem.tryAcquire();
    } else if (key.attachment() instanceof DataTransferAttachment) {
      return tryAcquireSession(key);
    } else {
      return false;
    }
  }

  private boolean tryAcquireSession(SelectionKey key) {
    DataTransferAttachment attachment = (DataTransferAttachment) key.attachment();
    return tryAcquireSession(attachment.userId + attachment.fileName);
  }

  private boolean tryAcquireSession(String idFileNameTuple) {
    Semaphore sem = idFileNameSemaphoreMap.get(idFileNameTuple);
    return sem == null ? false : sem.tryAcquire();
  }

  private boolean acquireSession(String idFileNameTuple) {
    Semaphore sem = idFileNameSemaphoreMap.get(idFileNameTuple);
    if (sem == null) {
      return false;
    } else {
      try {
        sem.acquire();
      } catch (InterruptedException e) {
        throw new IllegalStateException("Should not reach here.");
      }
      return true;
    }
  }

  public void release(SelectionKey key) {
    if (key.attachment() instanceof UploadRequestAttachment) {
      Semaphore sem = uploadRequestKeySemaphoreMap.get(key);
      if (sem != null) {
        sem.release();
      }
    } else if (key.attachment() instanceof DataTransferAttachment) {
      releaseSession(key);
    }
  }

  private void releaseSession(SelectionKey key) {
    DataTransferAttachment attachment = (DataTransferAttachment) key.attachment();
    releaseSession(attachment.userId + attachment.fileName);
  }

  private void releaseSession(String idFileNameTuple) {
    Semaphore sem = idFileNameSemaphoreMap.get(idFileNameTuple);
    if (sem != null) {
      sem.release();
    }
  }

  boolean registerConnectionForInitialization(SocketChannel connection) {
    UploadRequestAttachment attachment = new UploadRequestAttachment();
    SelectionKey key = null;
    try {
      key = initializationMultiplexor.registerConnection(connection, SelectionKey.OP_READ,
          attachment);
    } catch (ClosedChannelException e) {
      log.log(Level.WARNING, "Unexpectedly tried to register a closed channel.\n", e);
      return false;
    }
    uploadRequestKeySemaphoreMap.put(key, new Semaphore(1));
    int instantActiveConnections = activeConnections.incrementAndGet();
    log.log(Level.INFO, "There are {0} active connections.\n", instantActiveConnections);
    return true;
  }

  public int processFileUploadRequest(SelectionKey key) {
    SocketChannel connection = (SocketChannel) key.channel();
    UploadRequestAttachment attachment = (UploadRequestAttachment) key.attachment();
    UploadRequestDetails details = null;
    try {
      details = UploadRequestDetails.from(attachment);
    } catch (IllegalArgumentException e) {
      tryCloseConnection(connection);
      return -1;
    }
    String idFileNameTuple = details.id + details.fileName;
    Path userDir = Path.of(basePath, details.id);
    Path filePath = Path.of(basePath, details.id, details.fileName);
    DataTransferAttachment newAttachment = null;
    FileChannel file = null;
    int position = -1;
    try {
      if (!Files.exists(userDir)) {
        Files.createDirectories(userDir);
      }

      boolean b = acquireSession(idFileNameTuple);
      if (b) {
        SelectionKey currKey = dataTransferCurrentKeyMap.get(idFileNameTuple);
        DataTransferAttachment existingAttachment = (DataTransferAttachment) currKey.attachment();
        newAttachment = existingAttachment;
        position = (int) existingAttachment.totalBytesRead;
        if (dataTransferStatusMap.get(idFileNameTuple) == DataTransferStatus.DATA_TRANSFER) {
          log.log(Level.WARNING, "Currently established connection will be aborted.\n");
          cancelKey(currKey);
        } else {
          newAttachment.fileChannel = FileIOUtil.openFile(filePath, OpenOptionChoices.APPEND_EXISTING);
        }
      } else {
        position = 0;
        if (Files.exists(filePath)) {
          file = FileIOUtil.openFile(filePath, OpenOptionChoices.TRUNCATE_EXISTING);
        } else {
          file = FileIOUtil.openFile(filePath, OpenOptionChoices.OPEN_NEW);
        }
        newAttachment = new DataTransferAttachment(details.id, details.fileName, details.fileSize, file);
      }
      registerConnectionForDataTransfer(connection, newAttachment, details.reconnect || b);
      if (b)
        releaseSession(idFileNameTuple);
      uploadRequestKeySemaphoreMap.remove(key);
      return position;
    } catch (IOException e) {
      log.log(Level.WARNING, "File could not be opened. Aborting connection.\n");
      tryCloseConnection(connection);
      return -1;
    }
  }

  private void registerConnectionForDataTransfer(SocketChannel connection,
      DataTransferAttachment attachment, boolean reconnect) {
    SelectionKey key = null;
    try {
      key = uploadMultiplexor.registerConnection(connection, SelectionKey.OP_READ, attachment);
    } catch (ClosedChannelException e) {
      log.log(Level.WARNING, "Unexpectedly tried to register a closed channel.\n", e);
      return;
    }
    String idFileNameTuple = attachment.userId + attachment.fileName;
    if (!reconnect) {
      idFileNameSemaphoreMap.put(idFileNameTuple, new Semaphore(1));
    }
    dataTransferCurrentKeyMap.put(idFileNameTuple, key);
    dataTransferStatusMap.put(idFileNameTuple, DataTransferStatus.DATA_TRANSFER);
  }

  public void reportInitializationError(SelectionKey key) {
    cancelKey(key);
    uploadRequestKeySemaphoreMap.remove(key);
  }

  public void reportDataTransferError(SelectionKey key) {
    cancelKey(key);
    DataTransferAttachment attachment = ((DataTransferAttachment) key.attachment());
    try {
      ChannelIOUtil.flushAndClose(attachment.fileChannel, attachment.buffer);
    } catch (IOException e) {
    }
    dataTransferStatusMap.put(attachment.userId + attachment.fileName, DataTransferStatus.CLOSED);
  }

  public void reportDataTransferCompletion(SelectionKey key) {
    DataTransferAttachment attachment = (DataTransferAttachment) key.attachment();
    reportDataTransferCompletion(attachment.userId + attachment.fileName);
  }

  private void reportDataTransferCompletion(String idFileNameTuple) {
    System.out.printf("Removing data transfer key for %s\n", idFileNameTuple);
    SelectionKey key = dataTransferCurrentKeyMap.get(idFileNameTuple);
    cancelKey(key);
    idFileNameSemaphoreMap.remove(idFileNameTuple);
    dataTransferStatusMap.remove(idFileNameTuple);
    dataTransferCurrentKeyMap.remove(idFileNameTuple);
    log.log(Level.INFO,
        "uploadRequestKeySemaphoreMap size: {0}, idFileNameSemaphoreMap size: {1}," +
            "dataTransferStatusMap size: {2}, dataTransferCurrentKeyMap size: {3}\n",
        uploadRequestKeySemaphoreMap.size(), idFileNameSemaphoreMap.size(),
        dataTransferStatusMap.size(), dataTransferCurrentKeyMap.size());
  }

  private void cancelKey(SelectionKey key) {
    key.cancel();
    tryCloseConnection((SocketChannel) key.channel());
    activeConnections.decrementAndGet();
  }

  private void tryCloseConnection(SocketChannel connection) {
    try {
      connection.close();
    } catch (IOException e) {

    }
  }

  int getConnectionLimit() {
    return maxActiveConnections - activeConnections.get();
  }

}
