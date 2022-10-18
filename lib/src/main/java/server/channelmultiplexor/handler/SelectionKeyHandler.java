package server.channelmultiplexor.handler;

import java.nio.channels.SelectionKey;
import java.util.function.Consumer;

import server.connection.ConnectionManager;

public abstract class SelectionKeyHandler implements Consumer<SelectionKey> {

    public abstract void accept(SelectionKey key);

    protected final ConnectionManager connectionManager = ConnectionManager.getInstance();

}
