package file.server.fileioutil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

import file.server.attachment.DataTransferAttachment;

public class FileIOUtil {
  private static final Logger log = Logger.getLogger(FileIOUtil.class.getName());

  public static void clearIfFull(DataTransferAttachment attachment) throws IOException {
    if (!attachment.buffer.hasRemaining()) {
      flushToChannel(attachment.buffer, attachment.out);
      attachment.bufferFlushes = attachment.bufferFlushes + 1;
    }
  }

  public static void flushAndClose(DataTransferAttachment attachment) throws IOException {
    flushToChannel(attachment.buffer, attachment.out);
    try {
      attachment.out.close();
    } catch (IOException e) {
      log.log(Level.WARNING, "File close attempt failed.\n", e);
    }
  }

  public static void flushToChannel(ByteBuffer buffer, FileChannel out) throws IOException {
    buffer.flip();
    while (buffer.hasRemaining()) {
      try {
        out.write(buffer);
      } catch (IOException e) {
        log.log(Level.SEVERE, "Error occurred during disk write.\n", e);
        throw e;
      }
    }
    buffer.clear();
  }
}
