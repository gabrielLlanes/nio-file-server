package server.attachment;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.UUID;

import server.niofileserver.NioFileServer;

public class UploadRequestAttachment {

    public static final int MIN_REQUEST_SIZE = 43;

    public static final int MAX_REQUEST_SIZE = 72;

    public ByteBuffer buffer = ByteBuffer.allocate(1);

    public byte requestSize = -1;

    public UploadRequestAttachment() {
        buffer.limit(1);
    }

    public static class UploadRequestDetails {
        public String id;
        public String fileName;
        public int fileSize;
        public boolean reconnect;

        private UploadRequestDetails(String id, String fileName, int fileSize, boolean reconnect) {
            this.id = id;
            this.fileName = fileName;
            this.fileSize = fileSize;
            this.reconnect = reconnect;
        }

        public static UploadRequestDetails from(UploadRequestAttachment attachment) {
            String id = parseId(attachment.buffer, 0);
            String fileName = parseFileName(attachment.buffer, NioFileServer.UUID_BYTE_ARRAY_LENGTH);
            int fileSize = attachment.buffer.getInt(NioFileServer.UUID_BYTE_ARRAY_LENGTH + 1 + fileName.length());
            boolean reconnect = attachment.buffer
                    .get(NioFileServer.UUID_BYTE_ARRAY_LENGTH + 1 + fileName.length() + Integer.BYTES) != 0 ? true
                            : false;
            if (fileSize <= 0) {
                throw new IllegalArgumentException("File size must be a positive integer.\n");
            }
            return new UploadRequestDetails(id, fileName, fileSize, reconnect);
        }

        private static String parseId(ByteBuffer buffer, int index) {
            byte idLength = NioFileServer.UUID_BYTE_ARRAY_LENGTH;
            byte[] uuidBytes = new byte[idLength];
            buffer.get(index, uuidBytes, 0, idLength);
            String id = new String(uuidBytes);
            UUID.fromString(id);
            return id;
        }

        private static String parseFileName(ByteBuffer buffer, int sizeIndex) {
            byte fileNameSize = buffer.get(sizeIndex);
            byte[] fileNameBytes = new byte[fileNameSize];
            buffer.get(sizeIndex + 1, fileNameBytes, 0, fileNameSize);
            String fileName = new String(fileNameBytes);
            Path.of(fileName);
            return fileName;
        }
    }
}
