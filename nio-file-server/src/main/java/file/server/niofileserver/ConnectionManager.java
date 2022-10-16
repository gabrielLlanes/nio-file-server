package file.server.niofileserver;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import file.server.attachment.InitializationAttachment;
import file.server.status.FileTransferStatus;

public class ConnectionManager {

  private final Logger log = Logger.getLogger(ConnectionManager.class.getName());

  private final NioFileServer fileServer;

  private final ConnectionRequestAcceptor connectionRequestAcceptor = new ConnectionRequestAcceptor();

  private final ScheduledExecutorService connectionAcceptionExecutor = Executors.newSingleThreadScheduledExecutor();

  private final Selector sel;

  private final ServerSocketChannel listener;

  private int MAX_ACTIVE_CONNECTIONS;

  private AtomicInteger activeConnections = new AtomicInteger(0);

  private ConcurrentMap<String, FileTransferStatus> fileTransferStatuses = new ConcurrentHashMap<>();

  private ConcurrentMap<String, SelectionKey> fileTransferCurrentKeyMap = new ConcurrentHashMap<>();

  private ConcurrentMap<SelectionKey, Boolean> keyInUse = new ConcurrentHashMap<>();

  ConnectionManager(NioFileServer fileServer, SocketAddress serverAddress) throws IOException {
    this.fileServer = fileServer;
    sel = Selector.open();
    listener = ServerSocketChannel.open();
    listener.configureBlocking(false);
    listener.bind(serverAddress);
    listener.register(sel, SelectionKey.OP_ACCEPT);
  }

  private void acceptConnections() {
    int n = getConnectionLimit();
    for (int i = 0; i < n;) {
      try {
        sel.select();
      } catch (ClosedChannelException e) {
        log.log(Level.SEVERE, "Selector should not be closed.\n", e);
        System.exit(1);
      } catch (IOException e) {
        log.log(Level.WARNING, "Problem occurred during selection.\n", e);
      }
      if (sel.selectedKeys().size() == 0) {
        log.info("Selected key set size was 0.\n");
      } else {
        SocketChannel connection = connectionRequestAcceptor.acceptConnectionRequest(listener);
        if (connection != null)
          registerConnection(connection);
      }
      sel.selectedKeys().clear();
    }
  }

  void start() {
    connectionAcceptionExecutor.scheduleWithFixedDelay(this::acceptConnections, 0, 5, TimeUnit.SECONDS);
  }

  private void registerConnection(SocketChannel connection) {
    InitializationAttachment initializationAttachment = new InitializationAttachment();
    SelectionKey initializationKey = fileServer.registerConnection(connection, SelectionKey.OP_READ,
        initializationAttachment);
    if (initializationKey != null) {
      keyInUse.put(initializationKey, false);
      int instantActiveConnections = activeConnections.incrementAndGet();
      if (instantActiveConnections % 10 == 0 && instantActiveConnections > 0) {
        log.info(String.format("There are %d active connections.\n", instantActiveConnections));
      }
    }
  }

  private void cancelKey(SelectionKey key) {

  }

  private int getConnectionLimit() {
    return MAX_ACTIVE_CONNECTIONS - activeConnections.get();
  }

}
