package org.apache.hadoop.io.serializer;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.LimitedPrivate({"HDFS","MapReduce"})
@InterfaceStability.Evolving
public interface Serialization<T>{
    boolean accept(Class<?> c);
    Serializer<T> getSerializer(Class<T> c);
    DeSerializer<T> getDeSerializer(Class<T> c);
}
