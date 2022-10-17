package server.channelmultiplexor;

import java.nio.channels.SelectionKey;
import java.util.Map;
import java.util.concurrent.Semaphore;

import server.connection.ConnectionManager;
import server.status.DataTransferStatus;

public abstract class SelectionKeyHandler {
  protected abstract void accept(SelectionKey arg0);

  protected final ConnectionManager connectionManager = ConnectionManager.getInstance();

  protected final Map<String, DataTransferStatus> dataTransferStatusMapView = connectionManager
      .getDataTransferStatusMapView();

  protected final Map<SelectionKey, Semaphore> keySemaphoreMapView = connectionManager
      .getKeySemaphoreMapView();

  protected final Map<String, Semaphore> connectionIDSempahoreMapView = connectionManager
      .getConnectionIDSempahoreMapView();

  protected final Map<String, SelectionKey> dataTransferCurrentKeyMapView = connectionManager
      .getDataTransferCurrentKeyMapView();
}
