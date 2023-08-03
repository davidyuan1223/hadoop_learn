package org.apache.hadoop.fs.statistics;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

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
    private transient Map<String ,Long> counters;
    @JsonProperty
    private transient Map<String ,Long> gauges;
    @JsonProperty
    private transient Map<String ,Long> minimums;
    @JsonProperty
    private transient Map<String ,Long> maximums;
    @JsonProperty("meanstatistics")
    private transient Map<String ,MeanStatistic> meanStatistics;

    public IOStatisticsSnapshot(){
        createMaps();
    }
    public IOStatisticsSnapshot(IOStatistics source){
        if (source != null) {
            snapshot(source);
        }else {
            createMaps();
        }
    }
    private synchronized void createMaps(){
        counters=new ConcurrentHashMap<>();
        gauges=new ConcurrentHashMap<>();
        minimums=new ConcurrentHashMap<>();
        maximums=new ConcurrentHashMap<>();
        meanStatistics=new ConcurrentHashMap<>();
    }

    public synchronized void clear(){
        counters.clear();
        gauges.clear();
        minimums.clear();
        maximums.clear();
        meanStatistics.clear();
    }

    public synchronized void snapshot(IOStatistics source){
        checkNotNull(source);
        counters=snapshotMap(source.counters());
    }

    @Override
    public Map<String, Long> counters() {
        return counters;
    }

    @Override
    public Map<String, Long> gauges() {
        return gauges;
    }

    @Override
    public Map<String, Long> minimums() {
        return minimums;
    }

    @Override
    public Map<String, Long> maximums() {
        return maximums;
    }

    @Override
    public Map<String, MeanStatistic> meanStatistics() {
        return meanStatistics;
    }

    @Override
    public boolean aggregate(@Nullable IOStatistics statistics) {
        return false;
    }
}
