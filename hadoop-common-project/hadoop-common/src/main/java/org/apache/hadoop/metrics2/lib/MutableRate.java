package org.apache.hadoop.metrics2.lib;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MutableRate extends MutableStat{
    MutableRate(String name,String description,boolean extended){
        super(name,description,"Ops","Time",extended);
    }
}
