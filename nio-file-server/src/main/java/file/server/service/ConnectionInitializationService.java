package file.server.service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.logging.Logger;

import file.server.attachment.DataTransferAttachment;
import file.server.attachment.InitializationAttachment;
import file.server.niofileserver.ConnectionManager;
import file.server.openoptionchoices.OpenOptionChoices;
import file.server.status.RegistrationStatus;

public class ConnectionInitializationService implements SelectionKeyHandler {

  private final Logger log = Logger.getLogger(ConnectionInitializationService.class.getName());

  private final Path BASE_PATH = Path.

  private final ConnectionManager connectionManager;

  private static final int UUID_BYTE_ARRAY_LENGTH = 36;

  private static final int DEFAULT_BUFFER_SIZE = 8192;

  public ConnectionInitializationService(ConnectionManager connectionManager) {
    this.connectionManager = connectionManager;
  }

  @Override
  public void accept(SelectionKey arg0) {

  }
  
  private void registerClient(SelectionKey key) {
    InitializationAttachment attachment = (InitializationAttachment) key.attachment();
    attachment.lastOperationMillis = System.currentTimeMillis();
    boolean errorOccurred = false;
    if (attachment.status == RegistrationStatus.READ_ID) {
      try {
        readID(key);
      } catch (IOException ex) {
        log.warning(String.format("Socket connection was made, but reading connection ID failed.\n"));
        errorOccurred = true;
      }

    } else if (attachment.status == RegistrationStatus.READ_FILE_SIZE) {
      try {
        readFileSize(key);
      } catch (IOException ex) {
        log.warning(String.format("Error occurred reading file size for connection %s.\n", attachment.connectionID));
        errorOccurred = true;
      }
    }
    if (attachment.status == RegistrationStatus.WRITE_POSITION) {
      try {
        writePosition(key);
      } catch (IOException ex) {
        log.warning(String
            .format("Error occurred writing the current amount of bytes read for connection %s.\n",
                attachment.connectionID));
        errorOccurred = true;
      }
    } else if (attachment.status == RegistrationStatus.VERIFY_POSITION) {
      try {
        verifyPosition(key);
      } catch (IOException ex) {
        log.warning(String.format("Error occurred verifying position for connection %s.\n", attachment.connectionID));
        errorOccurred = true;
      }
    }
    if (attachment.status == RegistrationStatus.FINALIZE_NEW) {
      try {
        finalizeNew(key);
      } catch (IOException ex) {
        log.warning(String.format("Failure to finalize the new connection %s.\n", attachment.connectionID));
        errorOccurred = true;
      }
    } else if (attachment.status == RegistrationStatus.FINALIZE_RESTABLISH) {
      DataTransferAttachment existing = (DataTransferAttachment) connectionCurrentKeyMap
          .get(attachment.connectionID).attachment();
      try {
        finalizeReestablish(key, existing);
      } catch (IOException ex) {
        log.warning(String.format("Failed to open existing file for connection %s/\n", attachment.connectionID));
        errorOccurred = true;
      }
    }
  }

  public void readID(SelectionKey key) throws IOException {
    InitializationAttachment att = (InitializationAttachment) key.attachment();
    SocketChannel ch = (SocketChannel) key.channel();
    if (att.idBuffer.hasRemaining()) {
      ch.read(att.idBuffer);
      att.connectionIDReads += 1;
    }
    if (!att.idBuffer.hasRemaining()) {
      byte[] idBytes = Arrays.copyOf(att.idBuffer.array(), UUID_BYTE_ARRAY_LENGTH);
      att.idBuffer.clear();
      att.connectionID = new String(idBytes);
      SelectionKey existingKey = null;
      if ((existingKey = connectionCurrentKeyMap.get(att.connectionID)) != null) {
        log.info(String.format("Read a connection id of existing connection %s.\n",
            att.connectionID));
        DataTransferAttachment existingAttachment = (DataTransferAttachment) existingKey.attachment();
        att.isReestablish = true;
        att.position = existingAttachment.totalBytesRead;
        att.posBuffer.putLong(existingAttachment.totalBytesRead);
        att.posBuffer.flip();
        if (connectionStatuses.get(att.connectionID) == FileTransferStatus.DATA_TRANSFER) {
          log.warning(String.format(
              "There seems to already be an open connection for ID %s. New connection will take precedence."
                  + "Attachment status of existing connection is %s\n",
              att.connectionID, existingAttachment));
          closeConnectionAndCancel(existingKey);
          try {
            flushAndClose(((DataTransferAttachment) existingKey.attachment()));
          } catch (IOException ex) {
            log.warning(String.format("Failed flush for connection %s.\n", att.connectionID));
          }
        }
      } else {
        att.isReestablish = false;
        att.position = 0;
        att.posBuffer.putLong(0);
        att.posBuffer.flip();
      }
      att.status = RegistrationStatus.READ_FILE_SIZE;
    }
  }

  private void readFileSize(SelectionKey key) throws IOException {
    InitializationAttachment att = (InitializationAttachment) key.attachment();
    SocketChannel ch = (SocketChannel) key.channel();
    if (att.sizeBuffer.hasRemaining()) {
      ch.read(att.sizeBuffer);
    }
    if (!att.sizeBuffer.hasRemaining()) {
      att.sizeBuffer.flip();
      att.fileSize = att.sizeBuffer.getLong();
      if (att.isReestablish) {
        DataTransferAttachment existing = (DataTransferAttachment) connectionCurrentKeyMap.get(att.connectionID)
            .attachment();
        if (att.fileSize != existing.fileSize) {
          att.isReestablish = false;
        }
      }
      att.status = RegistrationStatus.WRITE_POSITION;
      key.interestOps(SelectionKey.OP_WRITE);
    }
  }

  private  void finalizeNew(SelectionKey key) throws IOException {
    InitializationAttachment att = (InitializationAttachment) key.attachment();
    Path path = Path.of(
        BASE_PATH.toString(),
        String.format("connection-file-%s.txt", att.connectionID));
    FileChannel newOut = null;
    if (!Files.exists(path)) {
      try {
        newOut = (FileChannel) Files.newByteChannel(path, OpenOptionChoices.OPEN_NEW);
      } catch (IOException ex) {
        throw new IOException(String.format(
            "Error occurred during file open of newly created file: %s. Aborting connection.\n",
            att.connectionID), ex);
      }
    } else {
      log.info(String.format("Skipping file creation for %s. File will be overwritten.\n", att.connectionID));
      try {
        newOut = (FileChannel) Files.newByteChannel(path, OpenOptionChoices.TRUNCATE_EXISTING);
      } catch (IOException ex) {
        throw new IOException(String.format(
            "Error occurred during file open of existing created file: %s. Aborting connection.\n",
            att.connectionID), ex);
      }
    }
    DataTransferAttachment newAttachment = new DataTransferAttachment();
    newAttachment.lastOperationMillis = System.currentTimeMillis();
    newAttachment.buffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
    newAttachment.fileSize = att.fileSize;
    newAttachment.connectionID = att.connectionID;
    newAttachment.connectionIDReads = att.connectionIDReads;
    newAttachment.out = newOut;
    key.attach(newAttachment);
    connectionCurrentKeyMap.put(att.connectionID, key);
    connectionStatuses.put(att.connectionID, FileTransferStatus.DATA_TRANSFER);
    log.info(String.format("Connection %s has been newly established.\n", att.connectionID));
  }

  public  void writePosition(SelectionKey key) throws IOException {
    InitializationAttachment att = (InitializationAttachment) key.attachment();
    SocketChannel ch = (SocketChannel) key.channel();
    while (att.posBuffer.hasRemaining()) {
      ch.write(att.posBuffer);
    }
    if (!att.posBuffer.hasRemaining()) {
      att.posBuffer.clear();
      att.status = RegistrationStatus.VERIFY_POSITION;
      key.interestOps(SelectionKey.OP_READ);
    }
  }

  public  void verifyPosition(SelectionKey key) throws IOException {
    InitializationAttachment att = (InitializationAttachment) key.attachment();
    SocketChannel ch = (SocketChannel) key.channel();
    if (att.posBuffer.hasRemaining()) {
      ch.read(att.posBuffer);
    }
    if (!att.posBuffer.hasRemaining()) {
      att.posBuffer.flip();
      if (att.posBuffer.getLong() != att.position) {
        throw new IOException("Position failed to agree.");
      }
      att.status = att.isReestablish ? RegistrationStatus.FINALIZE_RESTABLISH : RegistrationStatus.FINALIZE_NEW;
    }
  }

  private  void finalizeReestablish(SelectionKey key,
      DataTransferAttachment existing) throws IOException {
    InitializationAttachment att = (InitializationAttachment) key.attachment();
    Path path = Path.of(
        BASE_PATH.toString(),
        String.format("connection-file-%s.txt", att.connectionID));
    FileChannel existingFileOut = (FileChannel) Files.newByteChannel(path, OpenOptionChoices.APPEND_EXISTING);
    existing.out = existingFileOut;
    existing.connectionIDReads += att.connectionIDReads;
    key.attach(existing);
    connectionCurrentKeyMap.put(att.connectionID, key);
    connectionStatuses.put(att.connectionID, FileTransferStatus.DATA_TRANSFER);
    if (existing.totalBytesRead == existing.fileSize) {
      writeAndClose(key);
    }
    log.info(String.format(
        "Connection %s has been reestablished.\n"
            + "Restored attachment state is %s\n",
        att.connectionID, existing));
  }

}
