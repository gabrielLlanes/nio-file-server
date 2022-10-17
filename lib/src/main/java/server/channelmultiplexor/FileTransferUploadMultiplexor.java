package server.channelmultiplexor;

public class FileTransferUploadMultiplexor extends FileTransferMultiplexor {

  public FileTransferUploadMultiplexor() {
    super(new UploadSelectionKeyHandler(), 10);
    connectionManager.setUploadMultiplexor(this);
  }

}
