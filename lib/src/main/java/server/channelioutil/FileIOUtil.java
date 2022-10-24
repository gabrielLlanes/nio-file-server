package server.channelioutil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import server.attachment.DataTransferAttachment;

public class FileIOUtil {

  private static final Logger log = Logger.getLogger(FileIOUtil.class.getName());

  public static FileChannel openFile(Path p, Set<OpenOption> options) throws IOException {
    return (FileChannel) Files.newByteChannel(p, options);
  }

  public static void clearIfFull(DataTransferAttachment attachment) throws IOException {
    if (!attachment.buffer.hasRemaining()) {
      flushToChannel(attachment.buffer, attachment.fileChannel);
      attachment.bufferFlushes = attachment.bufferFlushes + 1;
    }
  }

  public static void flushAndClose(DataTransferAttachment attachment) throws IOException {
    if (!attachment.fileChannel.isOpen()) {
      return;
    }
    flushToChannel(attachment.buffer, attachment.fileChannel);
    try {
      attachment.fileChannel.close();
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
