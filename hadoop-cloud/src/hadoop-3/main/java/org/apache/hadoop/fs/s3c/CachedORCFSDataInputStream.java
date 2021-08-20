package org.apache.hadoop.fs.s3c;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

public class CachedORCFSDataInputStream extends FSDataInputStream {

  /**
   * This is a cache for this input stream.
   */
  private final ConcurrentHashMap<ImmutablePair<Long, Integer>, byte[]> cache =
      new ConcurrentHashMap<>();

  public CachedORCFSDataInputStream(InputStream in) {
    super(in);
  }

  /**
   * Apache ORC uses this API only and always `offset` is 0.
   */
  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    if (offset == 0 && length < 1024) {
      ImmutablePair<Long, Integer> key = new ImmutablePair<>(position, length);
      if (cache.containsKey(key)) {
        System.arraycopy(cache.get(key), 0, buffer, 0, length);
      } else {
        super.readFully(position, buffer, offset, length);
        cache.put(key, Arrays.copyOf(buffer, length));
      }
    } else {
      super.readFully(position, buffer, offset, length);
    }
  }
}
