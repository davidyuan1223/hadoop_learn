package org.apache.hadoop.metrics2;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.util.Collection;
import java.util.Collections;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface MetricsRecord {
    long timestamp();
    String name();
    String description();
    String context();
    Collection<MetricsTag> tags();
    Iterable<AbstractMetric> metrics();
}
