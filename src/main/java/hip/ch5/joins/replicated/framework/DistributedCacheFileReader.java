package hip.ch5.joins.replicated.framework;

import java.io.File;
import java.io.IOException;

public interface DistributedCacheFileReader<K, V> extends Iterable<Pair<K, V>> {
  public void init(File f) throws IOException;
  public void close();
}
