package server.channelmultiplexor;

import server.channelmultiplexor.handler.InitializationSelectionKeyHandler;

public class FileTransferInitializationMultiplexor extends FileTransferMultiplexor {

  public FileTransferInitializationMultiplexor() {
    super(new InitializationSelectionKeyHandler(), Runtime.getRuntime().availableProcessors());
    connectionManager.setInitializationMultiplexor(this);
  }

}
