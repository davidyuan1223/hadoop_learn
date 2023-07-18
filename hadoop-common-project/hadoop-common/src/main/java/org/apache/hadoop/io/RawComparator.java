package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.util.Comparator;
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface RawComparator<T> extends Comparator<T> {

    int compare(byte[] data1,int start1,int length1,byte[] data2,int start2,int length2);
}
