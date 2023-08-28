package org.apache.hadoop.metrics2.impl;

import java.util.ArrayList;

class MetricsBufferBuilder extends ArrayList<MetricsBuffer.Entry> {
    boolean add(String name,Iterable<MetricsRecordImpl> records){
        return add(new MetricsBuffer.Entry(name,records));
    }
    MetricsBuffer get(){
        return new MetricsBuffer(this);
    }
}
