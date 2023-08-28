package org.apache.hadoop.util;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface Tool extends Configurable {
    int run(String [] args)throws Exception;
}
