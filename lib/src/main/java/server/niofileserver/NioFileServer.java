package server.niofileserver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import server.channelmultiplexor.FileTransferMultiplexor;
import server.connection.ConnectionListener;
import server.connection.ConnectionManager;

public final class NioFileServer implements Runnable {

  public static final SocketAddress serverAddress = new InetSocketAddress(System.getenv("NIO_FILE_SERVER_ADDRESS"),
      11500);

  private final ConnectionListener connectionListener;

  private FileTransferMultiplexor fileTransferInitializationMultiplexor;

  private FileTransferMultiplexor fileTransferUploadMultiplexor;

  private boolean initializationMultiplexorSet = false;

  private boolean uploadMultiplexorSet = false;

  public NioFileServer() throws IOException {
    this.connectionListener = new ConnectionListener(serverAddress, ConnectionManager.getInstance());
  }

  public NioFileServer(SocketAddress serverAddress) throws IOException {
    this.connectionListener = new ConnectionListener(serverAddress, ConnectionManager.getInstance());
  }

  public void setFileTransferInitializationMultiplexor(
      FileTransferMultiplexor fileTransferInitializationMultiplexor) {
    this.fileTransferInitializationMultiplexor = fileTransferInitializationMultiplexor;
    initializationMultiplexorSet = true;
  }

  public void setFileTransferUploadMultiplexor(FileTransferMultiplexor fileTransferUploadMultiplexor) {
    this.fileTransferUploadMultiplexor = fileTransferUploadMultiplexor;
    uploadMultiplexorSet = true;
  }

  @Override
  public final void run() {
    if (!(initializationMultiplexorSet && uploadMultiplexorSet)) {
      throw new IllegalStateException("Multiplexors have not yet been set.");
    }
    connectionListener.bindListener();
    connectionListener.run();
    fileTransferInitializationMultiplexor.run();
    fileTransferUploadMultiplexor.run();
  }
}
