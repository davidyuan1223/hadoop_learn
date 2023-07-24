package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.util.Comparator;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/23
 **/
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface RawComparator<T> extends Comparator<T> {
    int compare(byte[] b1,int start1,int length1,
                byte[] b2,int start2,int length2);
}
