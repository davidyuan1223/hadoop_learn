package org.apache.hadoop.fs.statistics;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class IOStatisticsSnapshot implements IOStatistics, Serializable,IOStatisticsAggregator {
    private static final long serialVersionUID=-1762522703841538084L;
    private static final Class[] DESERIALIZATION_CLASSES={
            IOStatisticsSnapshot.class,
            TreeMap.class,
            Long.class,
            MeanStatistic.class
    };
    @JsonProperty
    private transient Map<String ,Long>
}
