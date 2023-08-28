package org.apache.hadoop.metrics2.impl;

import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricType;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsVisitor;

public class MetricGaugeLong extends AbstractMetric {
    final long value;
    MetricGaugeLong(MetricsInfo info, long value){
        super(info);
        this.value=value;
    }

    @Override
    public Long value() {
        return value;
    }

    @Override
    public MetricType type() {
        return MetricType.GAUGE;
    }

    @Override
    public void visit(MetricsVisitor visitor) {
        visitor.gauge(this,value);
    }
}
