package server.attachment;

import java.nio.ByteBuffer;

import server.NioFileServerPreRefactor;
import server.status.RegistrationStatus;

public class InitializationAttachment {
    public RegistrationStatus status = RegistrationStatus.READ_ID;
    public ByteBuffer idBuffer = ByteBuffer.allocate(NioFileServerPreRefactor.UUID_BYTE_ARRAY_LENGTH);
    public long connectionIDReads = 0;
    public String connectionID = null;
    public ByteBuffer posBuffer = ByteBuffer.allocate(Long.BYTES);
    public long position;
    public ByteBuffer sizeBuffer = ByteBuffer.allocate(Long.BYTES);
    public long fileSize;
    public boolean isReestablish = false;
    public long lastOperationMillis;
}
