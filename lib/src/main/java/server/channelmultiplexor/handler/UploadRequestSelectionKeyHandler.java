package server.channelmultiplexor.handler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;

import server.attachment.UploadRequestAttachment;
import server.channelioutil.ChannelIOUtil;
import server.connection.ConnectionManager;

public class UploadRequestSelectionKeyHandler extends SelectionKeyHandler {

  private static final Logger log = System.getLogger(UploadRequestSelectionKeyHandler.class.getName());

  public UploadRequestSelectionKeyHandler(ConnectionManager connectionManager) {
    super(connectionManager);
  }

  @Override
  public void accept(SelectionKey key) {
    SocketChannel connection = (SocketChannel) key.channel();
    UploadRequestAttachment attachment = (UploadRequestAttachment) key.attachment();
    try {
      if (attachment.requestSize == -1) {
        if (ChannelIOUtil.read(connection, attachment.buffer)) {
          attachment.buffer.flip();
          attachment.requestSize = attachment.buffer.get();
          if (attachment.requestSize >= UploadRequestAttachment.MIN_REQUEST_SIZE
              && attachment.requestSize <= UploadRequestAttachment.MAX_REQUEST_SIZE) {
            attachment.buffer = ByteBuffer.allocate(attachment.requestSize);
          } else {
            connectionManager.reportInitializationError(key);
            return;
          }
        }
      } else if (ChannelIOUtil.read(connection, attachment.buffer)) {
        key.cancel();
        int position = connectionManager.processFileUploadRequest(key);
        if (position >= 0) {
          ByteBuffer positionBuf = ByteBuffer.allocate(Integer.BYTES);
          positionBuf.putInt(position);
          ChannelIOUtil.flushToChannel(connection, positionBuf);
        }
      }
    } catch (IOException e) {
      log.log(Level.WARNING, "Reading of initialization data failed.\n", e);
      connectionManager.reportInitializationError(key);
    } finally {
      connectionManager.release(key);
    }
  }
}
