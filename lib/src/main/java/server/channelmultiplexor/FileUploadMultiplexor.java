package server.channelmultiplexor;

import server.channelmultiplexor.handler.UploadSelectionKeyHandler;
import server.connection.ConnectionManager;

public class FileUploadMultiplexor extends ConnectionKeyMultiplexor {

  public FileUploadMultiplexor(ConnectionManager connectionManager) {
    super(connectionManager, new UploadSelectionKeyHandler(connectionManager),
        Runtime.getRuntime().availableProcessors() * 2);
    connectionManager.setUploadMultiplexor(this);
  }

}
