package org.apache.spark.sql.connector.write;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * An aggregator of partition metrics collected during write operations.
 * <p>
 * This is patterned after {@code org.apache.spark.util.AccumulatorV2}
 * </p>
 */
public class PartitionMetricsWriteInfo {

  private final Map<String, PartitionMetrics> metrics = new TreeMap<>();

  /**
   * Merges another same-type accumulator into this one and update its state, i.e. this should be
   * merge-in-place.
   *
   * @param otherAccumulator Another object containing aggregated partition metrics
   */
  public void merge (PartitionMetricsWriteInfo otherAccumulator) {
    otherAccumulator.metrics.forEach((p, m) ->
        metrics.computeIfAbsent(p, key -> new PartitionMetrics(0L, 0L, 0))
            .merge(m));
  }

  /**
   * Update the partition metrics for the specified path by adding to the existing state.  This will
   * add the partition if it has not been referenced previously.
   *
   * @param partitionPath The path for the written partition
   * @param bytes The number of additional bytes
   * @param records the number of addition records
   * @param files the number of additional files
   */
  public void update(String partitionPath, long bytes, long records, int files) {
    metrics.computeIfAbsent(partitionPath, key -> new PartitionMetrics(0L, 0L, 0))
        .merge(new PartitionMetrics(bytes, records, files));
  }

  /**
   * Update the partition metrics for the specified path by adding to the existing state from an
   * individual file.  This will add the partition if it has not been referenced previously.
   *
   * @param partitionPath The path for the written partition
   * @param bytes The number of additional bytes
   * @param records the number of addition records
   */
  public void updateFile(String partitionPath, long bytes, long records) {
    update (partitionPath, bytes, records, 1);
  }

  /**
   * Convert this instance into an immutable {@code java.util.Map}.  This is used for posting to the
   * listener bus
   *
   * @return an immutable map of partition paths to their metrics
   */
  public Map<String, PartitionMetrics> toMap() {
    return Collections.unmodifiableMap(metrics);
  }

  /**
   * Returns if this accumulator is zero value or not. For a map accumulator this indicates if the
   * map is empty.
   *
   * @return {@code true} if there are no partition metrics
   */
  boolean isZero() {
    return metrics.isEmpty();
  }

  @Override
  public String toString() {
    return "PartitionMetricsWriteInfo{" +
        "metrics=" + metrics +
        '}';
  }
}
