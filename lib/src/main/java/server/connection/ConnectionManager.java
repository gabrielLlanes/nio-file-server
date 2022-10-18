package server.connection;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import server.attachment.DataTransferAttachment;
import server.attachment.InitializationAttachment;
import server.channelmultiplexor.FileTransferMultiplexor;
import server.fileioutil.FileIOUtil;
import server.status.DataTransferStatus;

public class ConnectionManager {

  private final Logger log = Logger.getLogger(ConnectionManager.class.getName());

  private static final ConnectionManager instance = new ConnectionManager();

  private FileTransferMultiplexor initializationMultiplexor;

  private FileTransferMultiplexor uploadMultiplexor;

  private int maxActiveConnections = 10_000;

  private AtomicInteger activeConnections = new AtomicInteger(0);

  private final ConcurrentMap<String, DataTransferStatus> dataTransferStatusMap = new ConcurrentHashMap<>();

  private final ConcurrentMap<SelectionKey, Semaphore> keySemaphoreMap = new ConcurrentHashMap<>();

  private final ConcurrentMap<String, Semaphore> connectionIDSempahoreMap = new ConcurrentHashMap<>();

  private final ConcurrentMap<String, SelectionKey> dataTransferCurrentKeyMap = new ConcurrentHashMap<>();

  private ConnectionManager() {
  }

  public static ConnectionManager getInstance() {
    return instance;
  }

  public void setMaxConnections(int n) {
    maxActiveConnections = n;
  }

  public void setInitializationMultiplexor(FileTransferMultiplexor initializationMultiplexor) {
    this.initializationMultiplexor = initializationMultiplexor;
  }

  public void setUploadMultiplexor(FileTransferMultiplexor uploadMultiplexor) {
    this.uploadMultiplexor = uploadMultiplexor;
  }

  public Semaphore getConnectionIDSemaphore(String connectionID) {
    return connectionIDSempahoreMap.get(connectionID);
  }

  public boolean tryAcquireKeySemaphore(SelectionKey key) {
    Semaphore s = keySemaphoreMap.get(key);
    if (s == null) {
      return false;
    }
    return s.tryAcquire();
  }

  public void releaseKeySemaphore(SelectionKey key) {
    keySemaphoreMap.get(key).release();
  }

  public DataTransferStatus getDataTransferStatus(String connectionID) {
    return dataTransferStatusMap.get(connectionID);
  }

  public DataTransferAttachment getDataTransferCurrentAttachment(String connectionID) {
    return (DataTransferAttachment) dataTransferCurrentKeyMap.get(connectionID).attachment();
  }

  boolean registerConnectionForInitialization(SocketChannel connection) {
    InitializationAttachment initializationAttachment = new InitializationAttachment();
    SelectionKey key = null;
    try {
      key = initializationMultiplexor.registerConnection(connection, SelectionKey.OP_READ,
          initializationAttachment);
    } catch (ClosedChannelException e) {
      log.log(Level.WARNING, "Unexpectedly tried to register a closed channel.\n", e);
      return false;
    }
    keySemaphoreMap.put(key, new Semaphore(1));
    int instantActiveConnections = activeConnections.incrementAndGet();
    log.info(String.format("There are %d active connections.\n", instantActiveConnections));
    return true;
  }

  public void registerConnectionForDataTransfer(SocketChannel connection,
      DataTransferAttachment attachment) {
    SelectionKey key = null;
    try {
      key = uploadMultiplexor.registerConnection(connection, SelectionKey.OP_READ, attachment);
    } catch (ClosedChannelException e) {
      log.log(Level.WARNING, "Unexpectedly tried to register a closed channel.\n", e);
      return;
    }
    keySemaphoreMap.put(key, new Semaphore(1));
    dataTransferCurrentKeyMap.put(attachment.connectionID, key);
    dataTransferStatusMap.put(attachment.connectionID, DataTransferStatus.DATA_TRANSFER);
  }

  public void reportInitializationError(SelectionKey key) {
    cancelKey(key);
    keySemaphoreMap.remove(key);
  }

  public void reportDataTransferError(SelectionKey key) {
    cancelKey(key);
    DataTransferAttachment attachment = ((DataTransferAttachment) key.attachment());
    try {
      FileIOUtil.flushAndClose((DataTransferAttachment) key.attachment());
    } catch (IOException e) {
    }
    dataTransferStatusMap.put(attachment.connectionID, DataTransferStatus.CLOSED);
    keySemaphoreMap.remove(key);
  }

  public void reportDataTransferReestablish(String connectionID, SocketChannel connection) {
    SelectionKey key = dataTransferCurrentKeyMap.get(connectionID);
    cancelKey(key);
    DataTransferAttachment attachment = (DataTransferAttachment) key.attachment();
    registerConnectionForDataTransfer(connection, attachment);
  }

  public void reportDataTransferCompletion(String connectionID) {
    SelectionKey key = dataTransferCurrentKeyMap.get(connectionID);
    cancelKey(key);
    keySemaphoreMap.remove(key);
    dataTransferStatusMap.remove(connectionID);
    dataTransferCurrentKeyMap.remove(connectionID);
    activeConnections.decrementAndGet();
  }

  private void cancelKey(SelectionKey key) {
    key.cancel();
    SocketChannel connection = (SocketChannel) key.channel();
    try {
      connection.close();
    } catch (IOException ex) {
    }
  }

  int getConnectionLimit() {
    return maxActiveConnections - activeConnections.get();
  }

}
