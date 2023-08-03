package org.apache.hadoop.metrics2;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface MetricsInfo {
    String name();
    String description();
}
