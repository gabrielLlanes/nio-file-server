package server.channelioutil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;

public class ChannelIOUtil {

  public static boolean read(ByteChannel channel, ByteBuffer buffer) throws IOException {
    if (buffer.hasRemaining()) {
      return buffer.remaining() == channel.read(buffer);
    } else {
      return true;
    }
  }

  public static void flushToChannel(ByteChannel channel, ByteBuffer buffer) throws IOException {
    buffer.flip();
    while (buffer.hasRemaining()) {
      channel.write(buffer);
    }
    buffer.clear();
  }

  public static void flushIfFull(ByteChannel channel, ByteBuffer buffer) throws IOException {
    if (!buffer.hasRemaining()) {
      flushToChannel(channel, buffer);
    }
  }

  public static void flushAndClose(ByteChannel channel, ByteBuffer buffer) throws IOException {
    flushToChannel(channel, buffer);
    channel.close();
  }
}
