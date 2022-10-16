package file.server.service;

import java.nio.channels.SelectionKey;
import java.util.function.Consumer;

public interface SelectionKeyHandler extends Consumer<SelectionKey> {
  @Override
  public void accept(SelectionKey arg0);
}
