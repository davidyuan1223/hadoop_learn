package org.apache.hadoop.fs.statistics;

import com.apache.hadoop.classification.InterfaceStability;

@InterfaceStability.Unstable
public interface IOStatisticsSource {
    default IOStatistics getIOStatistics(){
        return null;
    }
}
