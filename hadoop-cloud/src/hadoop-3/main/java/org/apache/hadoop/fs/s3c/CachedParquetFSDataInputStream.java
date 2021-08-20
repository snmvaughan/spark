package org.apache.hadoop.fs.s3c;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

/**
 * To minimize the overhead, we only cache 8 bytes (footer length field + magic string) per file.
 */
public class CachedParquetFSDataInputStream extends FSDataInputStream {

    private ConcurrentHashMap<ImmutablePair<Long, Integer>, byte[]> map;

    public CachedParquetFSDataInputStream(InputStream in, ConcurrentHashMap<ImmutablePair<Long, Integer>, byte[]> map) {
        super(in);
        this.map = map;
    }

    /**
     * This is required to preserve Apache Parquet behavior.
     */
    @Override
    public InputStream getWrappedStream() {
        FSDataInputStream s = (FSDataInputStream) super.getWrappedStream();
        return s.getWrappedStream();
    }

    @Override
    public int read() throws IOException {
        long pos = getPos();
        ImmutablePair<Long, Integer> key = new ImmutablePair<>(pos, 1);
        if (map.containsKey(key)) {
            super.seek(pos + 1);
            return map.get(key)[0] & 0xff;
        } else {
            int value = super.read();
            if (value >= 0) {
                map.computeIfAbsent(key, k -> new byte[]{(byte) value});
            }
            return value;
        }
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        ImmutablePair<Long, Integer> key = new ImmutablePair<>(position, length);
        if (map.containsKey(key) && offset == 0) {
            System.arraycopy(map.get(key), 0, buffer, 0, length);
            super.seek(position + length);
        } else {
            super.readFully(position, buffer, offset, length);
            map.computeIfAbsent(key, k -> Arrays.copyOf(buffer, length));
        }
    }
}
