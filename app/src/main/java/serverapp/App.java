package serverapp;

import server.apiendpoint.FileServer;

public class App {
  public static void main(String[] args) {
    FileServer server = new FileServer();
    server.run();
  }
}
