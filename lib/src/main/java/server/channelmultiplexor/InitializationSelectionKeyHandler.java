package server.channelmultiplexor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

import server.attachment.DataTransferAttachment;
import server.attachment.InitializationAttachment;
import server.openoptionchoices.OpenOptionChoices;
import server.status.DataTransferStatus;
import server.status.RegistrationStatus;

public class InitializationSelectionKeyHandler extends SelectionKeyHandler {

  private static final Logger log = Logger.getLogger(InitializationSelectionKeyHandler.class.getName());

  private static final Path basePath = Path.of("client-files");

  private static final int UUID_BYTE_ARRAY_LENGTH = 36;

  private static final int DEFAULT_BUFFER_SIZE = 8192;

  static {
    try {
      Files.createDirectories(basePath);
    } catch (IOException e) {
      log.log(Level.SEVERE, "Could not create directory for which to store files in.\n", e);
      System.exit(1);
    }
  }

  @Override
  protected void accept(SelectionKey key) {
    InitializationAttachment attachment = (InitializationAttachment) key.attachment();
    attachment.lastOperationMillis = System.currentTimeMillis();
    boolean errorOccurred = false;
    String errorMessage = null;
    if (attachment.status == RegistrationStatus.READ_ID) {
      try {
        readID(key);
      } catch (IOException ex) {
        errorMessage = String.format("Socket connection was made, but reading connection ID failed.\n");
        errorOccurred = true;
      }

    } else if (attachment.status == RegistrationStatus.READ_FILE_SIZE) {
      try {
        readFileSize(key);
      } catch (IOException ex) {
        errorMessage = String.format("Error occurred reading file size for connection %s.\n", attachment.connectionID);
        errorOccurred = true;
      }
    }
    if (attachment.status == RegistrationStatus.WRITE_POSITION) {
      try {
        writePosition(key);
      } catch (IOException ex) {
        errorMessage = String
            .format("Error occurred writing the current amount of bytes read for connection %s.\n",
                attachment.connectionID);
        errorOccurred = true;
      }
    } else if (attachment.status == RegistrationStatus.VERIFY_POSITION) {
      try {
        verifyPosition(key);
      } catch (IOException ex) {
        errorMessage = String.format("Error occurred verifying position for connection %s.\n", attachment.connectionID);
        errorOccurred = true;
      }
    }
    if (attachment.status == RegistrationStatus.FINALIZE_NEW) {
      try {
        finalizeNew(key);
      } catch (IOException ex) {
        errorMessage = String.format("Failure to finalize the new connection %s.\n", attachment.connectionID);
        errorOccurred = true;
      }
    } else if (attachment.status == RegistrationStatus.FINALIZE_RESTABLISH) {
      DataTransferAttachment existing = (DataTransferAttachment) connectionManager
          .getDataTransferCurrentAttachment(attachment.connectionID);
      try {
        finalizeReestablish(key, existing);
      } catch (IOException ex) {
        errorMessage = String.format("Failed to open existing file for connection %s/\n", attachment.connectionID);
        errorOccurred = true;
      }
    }
    if (errorOccurred) {
      log.warning(errorMessage);
      connectionManager.reportInitializationError(key);
    } else {
      connectionManager.getKeySemaphore(key).release();
    }
  }

  private void readID(SelectionKey key) throws IOException {
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
      Semaphore connectionIDSemaphore = connectionManager.getConnectionIDSemaphore(att.connectionID);
      if (connectionIDSemaphore != null) {

      }
      if ((existingKey = connectionManager.getDataTransferCurrentKey(att.connectionID)) != null) {
        log.info(String.format("Read a connection id of existing connection %s.\n",
            att.connectionID));
        DataTransferAttachment existingAttachment = (DataTransferAttachment) existingKey.attachment();
        att.isReestablish = true;
        att.position = existingAttachment.totalBytesRead;
        att.posBuffer.putLong(existingAttachment.totalBytesRead);
        att.posBuffer.flip();
        if (connectionManager.getDataTransferStatus(att.connectionID) == DataTransferStatus.DATA_TRANSFER) {
          log.warning(String.format(
              "There seems to already be an open connection for ID %s. New connection will take precedence."
                  + "Attachment status of existing connection is %s\n",
              att.connectionID, existingAttachment));
          connectionManager.reportDataTransferReestablish(att.connectionID);
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
        DataTransferAttachment existing = (DataTransferAttachment) connectionManager
            .getDataTransferCurrentAttachment(att.connectionID);
        if (att.fileSize != existing.fileSize) {
          att.isReestablish = false;
        }
      }
      att.status = RegistrationStatus.WRITE_POSITION;
      key.interestOps(SelectionKey.OP_WRITE);
    }
  }

  private void finalizeNew(SelectionKey key) throws IOException {
    InitializationAttachment att = (InitializationAttachment) key.attachment();
    Path path = Path.of(
        basePath.toString(),
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
    key.cancel();
    connectionManager.registerConnectionForDataTransfer((SocketChannel) key.channel(), newAttachment);
    // connectionCurrentKeyMap.put(att.connectionID, key);
    // connectionStatuses.put(att.connectionID, DataTransferStatus.DATA_TRANSFER);
    log.info(String.format("Connection %s has been newly established.\n", att.connectionID));
  }

  private void writePosition(SelectionKey key) throws IOException {
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

  private void verifyPosition(SelectionKey key) throws IOException {
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

  private void finalizeReestablish(SelectionKey key,
      DataTransferAttachment existing) throws IOException {
    InitializationAttachment att = (InitializationAttachment) key.attachment();
    Path path = Path.of(
        basePath.toString(),
        String.format("connection-file-%s.txt", att.connectionID));
    FileChannel existingFileOut = (FileChannel) Files.newByteChannel(path, OpenOptionChoices.APPEND_EXISTING);
    existing.out = existingFileOut;
    existing.connectionIDReads += att.connectionIDReads;
    key.attach(existing);
    // connectionCurrentKeyMap.put(att.connectionID, key);
    // connectionStatuses.put(att.connectionID, DataTransferStatus.DATA_TRANSFER);
    // if (existing.totalBytesRead == existing.fileSize) {
    // writeAndClose(key);
    // }
    log.info(String.format(
        "Connection %s has been reestablished.\n"
            + "Restored attachment state is %s\n",
        att.connectionID, existing));
  }
}
