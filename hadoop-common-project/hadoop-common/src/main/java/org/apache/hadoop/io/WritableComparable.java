package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Stable
public interface WritableComparable<T> extends Writable,Comparable<T>{
}
