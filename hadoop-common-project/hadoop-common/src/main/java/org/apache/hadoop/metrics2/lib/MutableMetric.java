package org.apache.hadoop.metrics2.lib;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class MutableMetric {
    private volatile boolean changed=true;
    public abstract void snapshot(MetricsRecordBuilder builder, boolean all);
    protected void setChanged(){
        changed=true;
    }
    protected void clearChanged(){
        changed=false;
    }
    public boolean changed(){
        return changed;
    }
}
