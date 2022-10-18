package server.channelmultiplexor.handler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

import server.attachment.DataTransferAttachment;
import server.fileioutil.FileIOUtil;

public class UploadSelectionKeyHandler extends SelectionKeyHandler {

  private static final Logger log = Logger.getLogger(UploadSelectionKeyHandler.class.getName());

  @Override
  public void accept(SelectionKey key) {
    DataTransferAttachment attachment = (DataTransferAttachment) key.attachment();
    SocketChannel connection = (SocketChannel) key.channel();
    long bytesRead;
    try {
      bytesRead = connection.read(attachment.buffer);
    } catch (IOException e) {
      log.log(Level.WARNING, String.format("Read failed. Connection needs to be reestablished."
          + "Attachment status was %s", attachment), e);
      connectionManager.reportDataTransferError(key);
      return;
    }
    if (bytesRead == -1) {
      log.warning(String.format("Client-side EOF. Connection needs to be reestablished."
          + "Attachment status was %s", attachment));
      connectionManager.reportDataTransferError(key);
      return;
    }
    attachment.reads += 1;
    attachment.totalBytesRead += bytesRead;
    if (attachment.totalBytesRead == attachment.fileSize) {
      log.info(String.format("Connection %s: All bytes read.\n",
          attachment.connectionID));
      try {
        FileIOUtil.flushAndClose(attachment);
      } catch (IOException e) {
        log.log(Level.WARNING, String.format(
            "Error occurred during disk write for connection %s. Integrity of data cannot be ensured.",
            attachment.connectionID), e);
      }
      try {
        while (connection.write(ByteBuffer.wrap(new byte[] { -86 })) != 1) {
        }
      } catch (IOException e) {
        log.log(Level.WARNING, "Error occurred during final write acknowledgement.\n", e);
      }
      log.info(String.format("Wrote acknowledgement byte for connection %s. Final attachment status is %s\n",
          attachment.connectionID, attachment));
      attachment.buffer = null;
      connectionManager.reportDataTransferCompletion(attachment.connectionID);
      return;
    } else {
      try {
        FileIOUtil.clearIfFull(attachment);
      } catch (IOException ex) {
        log.severe(String.format("Error occurred during disk write for connection %s. Aborting connection.\n",
            attachment.connectionID));
        connectionManager.reportDataTransferError(key);
        return;
      }
    }
    connectionManager.releaseKeySemaphore(key);
  }
}
