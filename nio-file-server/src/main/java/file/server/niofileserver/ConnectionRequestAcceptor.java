package file.server.niofileserver;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConnectionRequestAcceptor {

  private final Logger log = Logger.getLogger(ConnectionRequestAcceptor.class.getName());

  ConnectionRequestAcceptor() {
  }

  public SocketChannel acceptConnectionRequest(ServerSocketChannel listener) {
    SocketChannel connection = null;
    try {
      connection = listener.accept();
      if (connection == null) {
        log.warning(String.format("Listening socket tried to accept connection, but no connection was made.\n"));
        return null;
      }
      connection.finishConnect();
      connection.configureBlocking(false);
    } catch (IOException e) {
      log.log(Level.WARNING, "Initial connection attempt failed.\n", e);
      return null;
    }
    return connection;
  }
}
