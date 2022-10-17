package server.channelmultiplexor;

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
  protected void accept(SelectionKey key) {
    DataTransferAttachment attachment = (DataTransferAttachment) key.attachment();
    attachment.lastOperationMillis = System.currentTimeMillis();
    SocketChannel connection = null;
    connection = (SocketChannel) key.channel();
    long bytesRead;
    try {
      bytesRead = connection.read(attachment.buffer);
    } catch (IOException e) {
      log.log(Level.WARNING, String.format("Read failed. Connection needs to be reestablished."
          + "Attachment status was %s", attachment), e);
      System.out.printf("Data read failed for connection %s. Closing connection.\n", attachment.connectionID);
      connectionManager.reportDataTransferError(key);
      keySemaphoreMapView.get(key).release();
      return;
    }
    if (bytesRead == -1) {
      log.warning("Unexpected EOF");
    } else {
      attachment.reads += 1;
      attachment.totalBytesRead += bytesRead;
      if (attachment.totalBytesRead == attachment.fileSize) {
        log.info(String.format("Connection %s: All bytes read.\n",
            attachment.connectionID));
        try {
          FileIOUtil.flushAndClose(attachment);
        } catch (IOException e) {
          log.log(Level.SEVERE, String.format(
              "Error occurred during disk write for connection %s. Integrity of data cannot be ensured.",
              attachment.connectionID), e);
        }
        // key.interestOps(SelectionKey.OP_WRITE);
        try {
          while (connection.write(ByteBuffer.wrap(new byte[] { -86 })) != 1) {
          }
        } catch (IOException e) {
          log.log(Level.WARNING, "Error occurred during final write acknowledgement.\n", e);
        }
        log.info(String.format("Wrote acknowledgement byte for connection %s.\n", attachment.connectionID));
        attachment.buffer = null;
        keySemaphoreMapView.get(key).release();
        return;
      } else {
        try {
          FileIOUtil.clearIfFull(attachment);
        } catch (IOException ex) {
          log.severe(String.format("Error occurred during disk write for connection %s. Aborting connection.\n",
              attachment.connectionID));
          connectionManager.reportDataTransferError(key);
          return;
        } catch (NullPointerException ex) {
          System.out.println("Null: " + attachment.buffer);
        }
      }
      if (attachment.reads % 2000 == 0) {
        log.info(String.format("Connection %s: Read #%d read %d bytes. Buffer now at position %d/%d." +
            " Total of %d KB read.\n", attachment.connectionID, attachment.reads,
            bytesRead,
            attachment.buffer.position(),
            attachment.buffer.limit(), attachment.totalBytesRead / 1024));
      }
      keySemaphoreMapView.get(key).release();
    }
  }

}
