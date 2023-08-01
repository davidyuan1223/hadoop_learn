package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/25
 **/
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface PathFilter {
    boolean accept(Path path);
}
