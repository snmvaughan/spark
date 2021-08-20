/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3c;

import com.google.common.cache.LoadingCache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * A cached S3AFileSystem for ORC and Parquet files.
 */
public class CachedS3AFileSystem extends S3AFileSystem {

  /**
   * Note that this wraps `S3AFileSystem.open` which already ignores `bufferSize` parameter.
   */
  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    if (f.getName().toLowerCase(Locale.ROOT).endsWith(".orc")) {
      return new CachedORCFSDataInputStream(super.open(f, bufferSize));
    } else if (f.getName().toLowerCase(Locale.ROOT).endsWith(".parquet")) {
      try {
        return new CachedParquetFSDataInputStream(super.open(f, bufferSize), cache.get(f));
      } catch (ExecutionException e) {
        // Fallback to the non-cached reader
        return super.open(f, bufferSize);
      }
    } else {
      return super.open(f, bufferSize);
    }
  }

  /**
   * This is used for Parquet only and timeout is 1 minute for safety.
   */
  private final LoadingCache<Path, ConcurrentHashMap<ImmutablePair<Long, Integer>, byte[]>> cache =
    CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES).build(
      new CacheLoader<Path, ConcurrentHashMap<ImmutablePair<Long, Integer>, byte[]>>() {
        @Override
        public ConcurrentHashMap<ImmutablePair<Long, Integer>, byte[]> load(Path key) {
          return new ConcurrentHashMap<>();
        }
      });
}
