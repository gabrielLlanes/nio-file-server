package server.attachment;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class DataTransferAttachment {
    public long connectionIDReads = 0;
    public String connectionID = null;
    public long fileSize = 0;
    public FileChannel out = null;
    public ByteBuffer buffer = null;
    public long reads = 0;
    public long totalBytesRead = 0;
    public long bufferFlushes = 0;
    public long lastOperationMillis;

    @Override
    public String toString() {
        return String.format(
                "{%s: %d reads, %d total KB read, %d total bytes read, %d connectionID reads}",
                connectionID,
                reads,
                totalBytesRead / 1024,
                totalBytesRead,
                connectionIDReads);
    }

    public String channelsString() {
        return String.format("Buffer: {%s}, FileChannel: {%s}", buffer.toString(), out.toString());
    }
}
