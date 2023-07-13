package org.apache.hadoop.conf;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class Configured implements Configurable{
    private Configuration conf;
    public Configured(){this(null);}
    public Configured(Configuration conf){setConf(conf);}

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
