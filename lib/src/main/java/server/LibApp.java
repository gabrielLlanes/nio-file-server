package server;

import java.io.IOException;
import java.net.InetSocketAddress;

import server.channelmultiplexor.FileTransferInitializationMultiplexor;
import server.channelmultiplexor.FileTransferMultiplexor;
import server.channelmultiplexor.FileTransferUploadMultiplexor;
import server.niofileserver.NioFileServer;

public class LibApp {
  public static void main(String[] args) throws IOException {
    FileTransferMultiplexor m1 = new FileTransferInitializationMultiplexor();
    FileTransferUploadMultiplexor m2 = new FileTransferUploadMultiplexor();
    NioFileServer server = new NioFileServer(new InetSocketAddress("localhost", 11500));
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
