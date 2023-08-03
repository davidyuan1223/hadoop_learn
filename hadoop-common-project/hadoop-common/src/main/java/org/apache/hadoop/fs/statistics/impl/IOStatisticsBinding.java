package org.apache.hadoop.fs.statistics.impl;

import com.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.statistics.IOStatistics;

public class IOStatisticsBinding {
    public static final String ENTRY_PATTERN="(%s=%s)";
    @VisibleForTesting
    public static final String NULL_SOURCE="()";

    private IOStatisticsBinding(){}

    public static IOStatistics fromStorageStatistics(StorageStatistics storageStatistics){

    }
}
