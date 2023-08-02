package org.apache.hadoop.fs.statistics;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import javax.annotation.Nullable;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface IOStatisticsAggregator {
    boolean aggregate(@Nullable IOStatistics statistics);
}
