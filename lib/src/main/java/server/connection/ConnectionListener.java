package server.connection;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConnectionListener implements Runnable {

  private final Logger log = Logger.getLogger(ConnectionListener.class.getName());

  private final ScheduledExecutorService connectionListenerThread = Executors.newSingleThreadScheduledExecutor();

  private final ServerSocketChannel listener;

  private boolean listenerBound = false;

  private final SocketAddress serverAddress;

  private final ConnectionManager connectionManager;

  public ConnectionListener(SocketAddress serverAddress, ConnectionManager connectionManager) throws IOException {
    this.connectionManager = connectionManager;
    this.serverAddress = serverAddress;
    listener = ServerSocketChannel.open();
    listener.configureBlocking(true);
  }

  @Override
  public void run() {
    connectionListenerThread.scheduleWithFixedDelay(this::acceptConnections, 0, 3, TimeUnit.SECONDS);
  }

  private void acceptConnections() {
    if (!listenerBound)
      throw new IllegalStateException("Listener has not yet been bound.");
    int n = connectionManager.getConnectionLimit();
    log.info("Connection listener accepting up to " + n + " connections.\n");
    for (int i = 0; i < n;) {
      i += acceptConnectionRequest() ? 1 : 0;
    }
  }

  public void bindListener() {
    while (true) {
      try {
        listener.bind(serverAddress);
        listenerBound = true;
        break;
      } catch (IOException e) {
        log.log(Level.WARNING, "Problem opening listening socket.\n");
        try {
          Thread.sleep(3000);
        } catch (InterruptedException e1) {
        }
      }
    }
  }

  private boolean acceptConnectionRequest() {
    SocketChannel connection = null;
    try {
      connection = listener.accept();
      if (connection == null) {
        log.warning(String.format("Listening socket tried to accept connection, but no connection was made.\n"));
        return false;
      }
      connection.finishConnect();
      connection.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
      connection.configureBlocking(false);
    } catch (IOException e) {
      log.log(Level.WARNING, "Initial connection attempt failed.\n", e);
      return true;
    }
    connectionManager.registerConnectionForInitialization(connection);
    return true;
  }
}
