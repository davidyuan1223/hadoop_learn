package org.apache.hadoop.conf;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/22
 **/
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface Configurable {
    void setConf(Configuration conf);
    Configuration getConf();
}
