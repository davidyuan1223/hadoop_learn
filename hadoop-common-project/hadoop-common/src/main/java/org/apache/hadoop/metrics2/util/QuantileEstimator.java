package org.apache.hadoop.metrics2.util;

import java.util.Map;

public interface QuantileEstimator {
    void insert(long value);
    Map<Quantile,Long> snapshot();
    long getCount();
    void clear();
}
