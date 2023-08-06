package org.apache.hadoop.fs.statistics;

public interface DurationTrackerFactory {
    default DurationTracker trackDuration(String key,long count){
        return IOStatisticsSupport.stubDurationTracker();
    }
    default DurationTracker trackDuration(String key){
        return trackDuration(key,1);
    }
}
