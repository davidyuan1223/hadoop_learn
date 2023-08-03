package org.apache.hadoop.metrics2.lib;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;

import java.util.Map;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MetricsRegistry {
    private final Map<String ,MutableMetric> metricsMap= Maps.newLinkedHashMap();
    private final Map<String , MetricsTag> tagsMap=Maps.newLinkedHashMap();
    private final MetricsInfo metricsInfo;
    public MetricsRegistry(String name){
        metricsInfo=Interns.info(name,name);
    }
    public MetricsRegistry(MetricsInfo info){
        metricsInfo=info;
    }
    public synchronized MutableMetric get(String name){
        return metricsMap.get(name);
    }
    public synchronized MetricsTag getTag(String name){
        return tagsMap.get(name);
    }
    public MutableCounterInt newCounter(String name,String desc,int iVal){
        return newCounter(Interns.info(name,desc),iVal);
    }
    public synchronized MutableCounterInt newCounter(MetricsInfo info,int iVal){
        checkMetricName(info.name());
        MutableCounterInt ret = new MutableCounterInt(info, iVal);
        metricsMap.put(info.name(),ret);
        return ret;
    }
    
}
