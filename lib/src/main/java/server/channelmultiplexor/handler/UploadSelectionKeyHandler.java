package server.channelmultiplexor.handler;

import java.io.IOException;
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
    try {
      long bytesRead = connection.read(attachment.buffer);
      if (bytesRead == -1) {
        log.warning(String.format("Client-side EOF. Connection needs to be reestablished."
            + "Attachment status was %s", attachment));
        connectionManager.reportDataTransferError(key);
      } else {
        attachment.reads += 1;
        attachment.totalBytesRead += bytesRead;
        if (attachment.totalBytesRead == attachment.fileSize) {
          log.info(String.format("Connection %s/%s: All bytes read.\n",
              attachment.userId, attachment.fileName));
          finishFileTransfer(connection, attachment);
          log.info(String.format("Wrote acknowledgement byte for connection %s/%s. Final attachment status is %s\n",
              attachment.userId, attachment.fileName, attachment));
          connectionManager.reportDataTransferCompletion(key);
        } else {
          ChannelIOUtil.flushIfFull(attachment.fileChannel, attachment.buffer);
        }
      }
    } catch (IOException e) {
      log.log(Level.WARNING,
          String.format("Read or flush or acknowledgement failed. Connection needs to be reestablished."
              + "Attachment status was %s", attachment),
          e);
      connectionManager.reportDataTransferError(key);
    } finally {
      connectionManager.release(key);
    }
  }

  private void finishFileTransfer(SocketChannel connection, DataTransferAttachment attachment) throws IOException {
    ChannelIOUtil.flushAndClose(attachment.fileChannel, attachment.buffer);
    // write an acknowledgement byte
    while (connection.write(ByteBuffer.wrap(new byte[] { -86 })) != 1) {
    }
  }
}
