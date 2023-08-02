package org.apache.hadoop.fs.statistics;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.util.Map;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface IOStatistics {
    Map<String ,Long> counters();
    Map<String ,Long> gauges();
    Map<String ,Long> minimums();
    Map<String,Long> maximums();
    Map<String ,MeanStatistic> meanStatistics();
    long MIN_UNSET_VALUE=-1;
    long MAX_UNSET_VALUE=-1;
}
