package org.apache.hadoop.metrics2.impl;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsFilter;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@InterfaceAudience.Private
@VisibleForTesting
public class MetricsCollectorImpl implements MetricsCollector,Iterable<MetricsRecordBuilderImpl> {
    private final List<MetricsRecordBuilderImpl> rbs= Lists.newArrayList();
    private MetricsFilter recordFilter,metricFilter;

    @Override
    public MetricsRecordBuilder addRecord(MetricsInfo info) {
        boolean acceptable=recordFilter==null||recordFilter.accepts(info.name());
        MetricsRecordBuilderImpl rb = new MetricsRecordBuilderImpl(this, info, recordFilter, metricFilter, acceptable);
        if (acceptable) rbs.add(rb);
        return rb;
    }

    @Override
    public MetricsRecordBuilder addRecord(String name) {
        return addRecord(Interns.info(name,name+" record"));
    }
    public List<MetricsRecordImpl> getRecords(){
        List<MetricsRecordImpl> recs = Lists.newArrayListWithCapacity(rbs.size());
        for (MetricsRecordBuilderImpl rb : rbs) {
            MetricsRecordImpl mr = rb.getRecord();
            if (mr != null) {
                recs.add(mr);
            }
        }
        return recs;
    }

    @Override
    public Iterator<MetricsRecordBuilderImpl> iterator() {
        return rbs.iterator();
    }
    @InterfaceAudience.Private
    public void clear(){
        rbs.clear();
    }
    MetricsCollectorImpl setRecordFilter(MetricsFilter rf){
        recordFilter=rf;
        return this;
    }
    MetricsCollectorImpl setMetricsFilter(MetricsFilter mf){
        metricFilter=mf;
        return this;
    }
}
