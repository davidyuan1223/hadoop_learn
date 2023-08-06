package org.apache.hadoop.util;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

@InterfaceStability.Unstable
@InterfaceAudience.Public
public interface IPList {
    boolean isIn(String ipAddress);
}
