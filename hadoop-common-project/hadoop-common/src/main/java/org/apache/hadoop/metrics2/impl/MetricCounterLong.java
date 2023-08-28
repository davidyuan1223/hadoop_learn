package org.apache.hadoop.metrics2.impl;

import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricType;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsVisitor;

class MetricCounterLong extends AbstractMetric {
    final long value;
    MetricCounterLong(MetricsInfo info, long value){
        super(info);
        this.value=value;
    }

    @Override
    public Long value() {
        return value;
    }

    @Override
    public MetricType type() {
        return MetricType.COUNTER;
    }

    @Override
    public void visit(MetricsVisitor visitor) {
        visitor.counter(this,value);
    }
}
