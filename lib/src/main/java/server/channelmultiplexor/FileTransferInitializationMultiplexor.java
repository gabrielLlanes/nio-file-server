package server.channelmultiplexor;

public class FileTransferInitializationMultiplexor extends FileTransferMultiplexor {

  public FileTransferInitializationMultiplexor() {
    super(new InitializationSelectionKeyHandler(), 2);
    connectionManager.setInitializationMultiplexor(this);
  }

}
