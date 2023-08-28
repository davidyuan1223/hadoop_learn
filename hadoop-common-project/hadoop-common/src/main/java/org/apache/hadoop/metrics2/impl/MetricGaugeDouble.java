package org.apache.hadoop.metrics2.impl;

import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricType;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsVisitor;

public class MetricGaugeDouble extends AbstractMetric {
    final double value;
    MetricGaugeDouble(MetricsInfo info,double value){
        super(info);
        this.value=value;
    }

    @Override
    public Double value() {
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
