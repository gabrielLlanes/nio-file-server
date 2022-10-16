package file.server.niofileserver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import file.server.attachment.InitializationAttachment;

public abstract class NioFileServer {

  private Selector sel;

  private ServerSocketChannel listener;

  private final ConnectionManager connectionRequestManager;

  protected NioFileServer(SocketAddress serverAddress) throws IOException {
    this.connectionRequestManager = new ConnectionManager(this, serverAddress);
  }

  protected void initializeServer() throws IOException {
    // sel = Selector.open();
    // listener = ServerSocketChannel.open();
    // listener.configureBlocking(false);
    // listener.bind(serverAddress);
    // listener.register(sel, SelectionKey.OP_ACCEPT);
  }

  protected abstract void runSelector();

  protected abstract void handleSelectedKey(SelectionKey key);

  abstract SelectionKey registerConnection(SocketChannel connection, int ops, InitializationAttachment attachment);

  public abstract void cancelKey(SelectionKey key);
}
