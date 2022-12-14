package server.apiendpoint;

import java.io.IOException;
import java.net.InetSocketAddress;

import server.channelmultiplexor.UploadRequestMultiplexor;
import server.RuntimeMetrics;
import server.channelmultiplexor.ConnectionKeyMultiplexor;
import server.channelmultiplexor.FileUploadMultiplexor;
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
    ConnectionManager connectionManager = ConnectionManager.getInstance();
    ConnectionKeyMultiplexor m1 = new UploadRequestMultiplexor(connectionManager);
    FileUploadMultiplexor m2 = new FileUploadMultiplexor(connectionManager);
    connectionManager.setMaxConnections(Integer.parseInt(System.getenv("NIO_FILE_SERVER_MAXIMUM_CONNECTIONS")));
    NioFileServer server = new NioFileServer(new InetSocketAddress(System.getenv("NIO_FILE_SERVER_ADDRESS"), 11500));
    server.setFileTransferInitializationMultiplexor(m1);
    server.setFileTransferUploadMultiplexor(m2);
    server.run();
    while (true) {
      try {
        Thread.sleep(5_000);
        RuntimeMetrics.logRuntimeMetrics();
      } catch (InterruptedException e) {
      }
    }
  }
}
