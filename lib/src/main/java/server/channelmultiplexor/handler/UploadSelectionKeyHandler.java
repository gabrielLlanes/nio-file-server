package server.channelmultiplexor.handler;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

import server.attachment.DataTransferAttachment;
import server.channelioutil.ChannelIOUtil;
import server.connection.ConnectionManager;

public class UploadSelectionKeyHandler extends SelectionKeyHandler {

  private static final Logger log = Logger.getLogger(UploadSelectionKeyHandler.class.getName());

  public UploadSelectionKeyHandler(ConnectionManager connectionManager) {
    super(connectionManager);
  }

  @Override
  public void accept(SelectionKey key) {
    DataTransferAttachment attachment = (DataTransferAttachment) key.attachment();
    SocketChannel connection = (SocketChannel) key.channel();
    long bytesRead = -2;
    try {
      bytesRead = connection.read(attachment.buffer);
    } catch (IOException e) {
      log.log(Level.WARNING, String.format("Read failed. Connection needs to be reestablished."
          + "Attachment status was %s", attachment), e);
      connectionManager.reportDataTransferError(key);
      connectionManager.release(key);
      return;
    } catch (BufferOverflowException e) {
      log.log(Level.WARNING,
          String.format("Connection %s/%s exception %s:", attachment.userId, attachment.fileName, attachment.buffer),
          e);
    }
    if (bytesRead == -1) {
      log.warning(String.format("Client-side EOF. Connection needs to be reestablished."
          + "Attachment status was %s", attachment));
      connectionManager.reportDataTransferError(key);
      connectionManager.release(key);
      return;
    }
    attachment.reads += 1;
    attachment.totalBytesRead += bytesRead;
    if (attachment.totalBytesRead == attachment.fileSize) {
      log.info(String.format("Connection %s/%s: All bytes read.\n",
          attachment.userId, attachment.fileName));
      finishFileTransfer(connection, attachment);
      log.info(String.format("Wrote acknowledgement byte for connection %s/%s. Final attachment status is %s\n",
          attachment.userId, attachment.fileName, attachment));
      connectionManager.reportDataTransferCompletion(attachment.userId + attachment.fileName);
      return;
    } else {
      try {
        ChannelIOUtil.flushIfFull(attachment.fileChannel, attachment.buffer);
      } catch (IOException e) {
        connectionManager.reportDataTransferError(key);
        connectionManager.release(key);
        return;
      }
    }
    connectionManager.release(key);
  }

  private void finishFileTransfer(SocketChannel connection, DataTransferAttachment attachment) {
    try {
      ChannelIOUtil.flushAndClose(attachment.fileChannel, attachment.buffer);
    } catch (IOException e) {
      log.log(Level.WARNING, String.format(
          "Error occurred during disk write for connection %s/%s. Integrity of data cannot be ensured.",
          attachment.userId, attachment.fileName), e);
    }
    try {
      while (connection.write(ByteBuffer.wrap(new byte[] { -86 })) != 1) {
      }
    } catch (IOException e) {
      log.log(Level.WARNING, "Error occurred during final write acknowledgement.\n", e);
    }
  }
}
