package org.apache.hadoop.fs.statistics;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.statistics.impl.StubDurationTracker;
import org.apache.hadoop.fs.statistics.impl.StubDurationTrackerFactory;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class IOStatisticsSupport {
    private IOStatisticsSupport(){}

    public static IOStatisticsSnapshot snapshotIOStatistics(IOStatistics statistics){
        return new IOStatisticsSnapshot(statistics);
    }

    public static IOStatisticsSnapshot snapshotIOStatistics(){
        return new IOStatisticsSnapshot();
    }

    public static IOStatistics retrieveIOStatistics(final Object source){
        if (source instanceof IOStatistics) {
            return (IOStatistics) source;
        } else if (source instanceof IOStatisticsSource) {
            return ((IOStatisticsSource)source).getIOStatistics();
        }else {
            return null;
        }
    }
    public static DurationTrackerFactory stubDurationTrackerFactory(){
        return StubDurationTrackerFactory.STUB_DURATION_TRACKER_FACTORY;
    }

    public static DurationTracker stubDurationTracker(){
        return StubDurationTracker.STUB_DURATION_TRACKER;
    }
}
