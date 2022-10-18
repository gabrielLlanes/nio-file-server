package server.channelmultiplexor;

import server.channelmultiplexor.handler.UploadSelectionKeyHandler;

public class FileTransferUploadMultiplexor extends FileTransferMultiplexor {

  public FileTransferUploadMultiplexor() {
    super(new UploadSelectionKeyHandler(), Runtime.getRuntime().availableProcessors() * 2);
    connectionManager.setUploadMultiplexor(this);
  }

}
