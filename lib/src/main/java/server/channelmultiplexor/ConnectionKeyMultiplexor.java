package server.channelmultiplexor;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import server.channelmultiplexor.handler.SelectionKeyHandler;
import server.connection.ConnectionManager;

public abstract class ConnectionKeyMultiplexor implements Runnable {

  protected final Logger log = Logger.getLogger(this.getClass().getName());

  protected Selector sel;

  private final ExecutorService pool;

  protected final SelectionKeyHandler selectionKeyHandler;

  protected final ConnectionManager connectionManager;

  public ConnectionKeyMultiplexor(ConnectionManager connectionManager, SelectionKeyHandler selectionKeyHandler,
      int nThreads) {
    this.connectionManager = connectionManager;
    this.selectionKeyHandler = selectionKeyHandler;
    pool = Executors.newFixedThreadPool(nThreads);
  }

  @Override
  public void run() {
    openSelector();
    new Thread(this::runSelector).start();
  }

  public final SelectionKey registerConnection(SocketChannel connection, int ops, Object attachment)
      throws ClosedChannelException {
    return connection.register(sel, ops, attachment);
  }

  private final void openSelector() {
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

  /*
   * Select with given timeout. If tryAcquire() fails, then we know that
   * processing of the key is being done by another thread, so we skip a
   * submission to the thread pool.
   */
  private final void runSelector() {
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
      } else {
        Iterator<SelectionKey> it = sel.selectedKeys().iterator();
        while (it.hasNext()) {
          try {
            SelectionKey key = it.next();
            if (connectionManager.tryAcquire(key)) {
              /*
               * The last thread that held the semaphore may have cancelled the key before
               * releasing it, so we need to check if the key is still valid before
               * proceeding.
               */
              if (key.isValid())
                pool.execute(() -> selectionKeyHandler.accept(key));
              else
                connectionManager.release(key);
            }
          } catch (CancelledKeyException e) {
            log.log(Level.WARNING, "Key was cancelled during handling.\n", e);
          }
          it.remove();
        }
      }
    }
  }
}
