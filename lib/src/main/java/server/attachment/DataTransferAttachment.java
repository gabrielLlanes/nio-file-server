package server.attachment;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class DataTransferAttachment {

    private static int DEFAULT_BUFFER_SIZE = 8192;

    public String userId;

    public String fileName;

    public long fileSize = 0;

    public FileChannel fileChannel = null;

    public ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);

    public long reads = 0;

    public long totalBytesRead = 0;

    public long bufferFlushes = 0;

    public DataTransferAttachment() {

    }

    public DataTransferAttachment(String id, String fileName, long fileSize, FileChannel fileChannel) {
        this.userId = id;
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.fileChannel = fileChannel;
    }

    @Override
    public String toString() {
        return String.format(
                "{%s/%s: %d reads, %d total KB read, %d total bytes read}",
                userId,
                fileName,
                reads,
                totalBytesRead / 1024,
                totalBytesRead);
    }

    public String channelsString() {
        return String.format("Buffer: {%s}, FileChannel: {%s}", buffer.toString(), fileChannel.toString());
    }
}
