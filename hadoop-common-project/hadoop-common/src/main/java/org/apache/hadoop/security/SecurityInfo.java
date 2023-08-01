package org.apache.hadoop.security;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

@InterfaceStability.Evolving
@InterfaceAudience.LimitedPrivate({"HDFS","MapReduce"})
public abstract class SecurityInfo {
    public abstract KerberosInfo getKerberosInfo(Class<?> protocol, Configuration conf);

    public abstract TokenInfo getTokenInfo(Class<?> protocol, Configuration conf);
}
