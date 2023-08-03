package org.apache.hadoop.metrics2.lib;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class MutableCounter extends MutableMetric{
    private final MetricsInfo info;
    protected MutableCounter(MetricsInfo info){
        this.info= Preconditions.checkNotNull(info,"counter info");
    }
    protected MetricsInfo info(){
        return info;
    }
    public abstract void incr();
}
