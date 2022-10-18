package server.apiendpoint;

import java.io.IOException;
import java.net.InetSocketAddress;

import server.channelmultiplexor.FileTransferInitializationMultiplexor;
import server.channelmultiplexor.FileTransferMultiplexor;
import server.channelmultiplexor.FileTransferUploadMultiplexor;
import server.connection.ConnectionManager;
import server.niofileserver.NioFileServer;

public class FileServer implements Runnable {

  public void run() {
    try {
      runServer();
    } catch (IOException e) {
      System.exit(1);
    }
  }

  private void runServer() throws IOException {
    FileTransferMultiplexor m1 = new FileTransferInitializationMultiplexor();
    FileTransferUploadMultiplexor m2 = new FileTransferUploadMultiplexor();
    ConnectionManager.getInstance()
        .setMaxConnections(Integer.parseInt(System.getenv("NIO_FILE_SERVER_MAXIMUM_CONNECTIONS")));
    NioFileServer server = new NioFileServer(new InetSocketAddress(System.getenv("NIO_FILE_SERVER_ADDRESS"), 11500));
    server.setFileTransferInitializationMultiplexor(m1);
    server.setFileTransferUploadMultiplexor(m2);
    server.run();
    while (true) {
      try {
        Thread.sleep(10_000);
      } catch (InterruptedException e) {
      }
    }
  }
}
