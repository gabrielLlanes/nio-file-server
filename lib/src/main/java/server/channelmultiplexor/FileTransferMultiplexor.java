package server.channelmultiplexor;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

import server.connection.ConnectionManager;

public abstract class FileTransferMultiplexor {

  protected final Logger log = Logger.getLogger(this.getClass().getName());

  protected Selector sel;

  protected final ExecutorService pool;

  protected final SelectionKeyHandler selectionKeyHandler;

  protected ConnectionManager connectionManager = ConnectionManager.getInstance();

  private Map<SelectionKey, Semaphore> keySemaphoreMapView;

  public FileTransferMultiplexor(SelectionKeyHandler selectionKeyHandler, int nThreads) {
    pool = Executors.newFixedThreadPool(nThreads);
    this.selectionKeyHandler = selectionKeyHandler;
    openSelector();
  }

  public final SelectionKey registerConnection(SocketChannel connection, int ops, Object attachment)
      throws ClosedChannelException {
    return connection.register(sel, ops, attachment);
  }

  public final void runSelector() {
    this.keySemaphoreMapView = connectionManager.getKeySemaphoreMapView();
    while (true) {
      try {
        sel.select(250);
      } catch (ClosedChannelException e) {
        log.log(Level.SEVERE, "Selector should not be closed.\n", e);
        System.exit(1);
      } catch (IOException e) {
        log.log(Level.WARNING, "Error occurred during selection.\n", e);
        continue;
      }
      if (sel.selectedKeys().size() == 0) {
        // log.info("Selected key set size was 0.\n");
      } else {
        Iterator<SelectionKey> it = sel.selectedKeys().iterator();
        while (it.hasNext()) {
          try {
            SelectionKey key = it.next();
            Semaphore keySemaphore = keySemaphoreMapView.get(key);
            if (keySemaphore != null && keySemaphore.tryAcquire()) {
              pool.execute(() -> selectionKeyHandler.accept(key));
            }
          } catch (CancelledKeyException e) {
            log.log(Level.WARNING, "Key was cancelled during handling.\n", e);
          }
          it.remove();
        }
      }
    }
  }

  private void openSelector() {
    while (true) {
      try {
        sel = Selector.open();
        break;
      } catch (IOException e) {
        log.log(Level.WARNING, "Error occurred opening selector.\n", e);
        try {
          Thread.sleep(2_000);
        } catch (InterruptedException e1) {
        }
      }
    }
  }
}
