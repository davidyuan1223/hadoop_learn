package org.apache.hadoop.metrics2;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface MetricsVisitor {
    void gauge(MetricsInfo info,int value);
    void gauge(MetricsInfo info,long value);
    void gauge(MetricsInfo info,float value);
    void gauge(MetricsInfo info,double value);
    void counter(MetricsInfo info,int value);
    void counter(MetricsInfo info,long value);
}
