package org.apache.hadoop.metrics2.impl;

import org.apache.hadoop.metrics2.*;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.thirdparty.com.google.common.collect.Collections2;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.util.Time;

import java.util.Collections;
import java.util.List;

class MetricsRecordBuilderImpl extends MetricsRecordBuilder {
    private final MetricsCollector parent;
    private final long timestamp;
    private final MetricsInfo recInfo;
    private final List<AbstractMetric> metrics;
    private final List<MetricsTag> tags;
    private final MetricsFilter recordFilter,metricFilter;
    private final boolean acceptable;

    MetricsRecordBuilderImpl(MetricsCollector parent,MetricsInfo info,
                             MetricsFilter rf,MetricsFilter mf,boolean acceptable){
        this.parent=parent;
        timestamp= Time.now();
        recInfo=info;
        metrics= Lists.newArrayList();
        tags=Lists.newArrayList();
        recordFilter=rf;
        metricFilter=mf;
        this.acceptable=acceptable;
    }

    @Override
    public MetricsCollector parent() {
        return parent;
    }

    @Override
    public MetricsRecordBuilder tag(MetricsInfo info, String value) {
        if (acceptable) {
            tags.add(Interns.tag(info,value));
        }
        return this;
    }

    @Override
    public MetricsRecordBuilder add(MetricsTag tag) {
        tags.add(tag);
        return this;
    }

    @Override
    public MetricsRecordBuilder add(AbstractMetric metric) {
        metrics.add(metric);
        return this;
    }

    @Override
    public MetricsRecordBuilder addCounter(MetricsInfo info, long value) {
        if (acceptable && (metricFilter==null || metricFilter.accepts(info.name()))){
            metrics.add(new MetricCounterLong(info,value));
        }
        return this;
    }

    @Override
    public MetricsRecordBuilder addCounter(MetricsInfo info, int value) {
        if (acceptable && (metricFilter==null || metricFilter.accepts(info.name()))){
            metrics.add(new MetricCounterInt(info,value));
        }
        return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo info, int value) {
        if (acceptable && (metricFilter==null || metricFilter.accepts(info.name()))){
            metrics.add(new MetricGaugeInt(info,value));
        }
        return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo info, long value) {
        if (acceptable && (metricFilter==null || metricFilter.accepts(info.name()))){
            metrics.add(new MetricGaugeLong(info,value));
        }
        return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo info, double value) {
        if (acceptable && (metricFilter==null || metricFilter.accepts(info.name()))){
            metrics.add(new MetricGaugeDouble(info,value));
        }
        return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo info, float value) {
        if (acceptable && (metricFilter==null || metricFilter.accepts(info.name()))){
            metrics.add(new MetricGaugeFloat(info,value));
        }
        return this;
    }

    @Override
    public MetricsRecordBuilder setContext(String value) {
        return tag(MsInfo.Context,value);
    }
    public MetricsRecordImpl getRecord(){
        if (acceptable && (recordFilter == null || recordFilter.accepts(tags))) {
            return new MetricsRecordImpl(recInfo,timestamp,tags(),metrics());
        }
        return null;
    }

    public List<MetricsTag> tags() {
        return Collections.unmodifiableList(tags);
    }

    public List<AbstractMetric> metrics() {
        return Collections.unmodifiableList(metrics);
    }
}
