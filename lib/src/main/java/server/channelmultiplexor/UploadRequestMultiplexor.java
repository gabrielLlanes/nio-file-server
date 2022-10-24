package server.channelmultiplexor;

import server.channelmultiplexor.handler.UploadRequestSelectionKeyHandler;
import server.connection.ConnectionManager;

public class UploadRequestMultiplexor extends ConnectionKeyMultiplexor {

  public UploadRequestMultiplexor(ConnectionManager connectionManager) {
    super(connectionManager, new UploadRequestSelectionKeyHandler(connectionManager),
        Runtime.getRuntime().availableProcessors());
    connectionManager.setInitializationMultiplexor(this);
  }

}
