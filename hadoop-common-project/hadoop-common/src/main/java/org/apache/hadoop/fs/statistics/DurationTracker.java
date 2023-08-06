package org.apache.hadoop.fs.statistics;

import java.time.Duration;

public interface DurationTracker extends AutoCloseable{
    void failed();
    void close();
    default Duration asDuration(){
        return Duration.ZERO;
    }
}
