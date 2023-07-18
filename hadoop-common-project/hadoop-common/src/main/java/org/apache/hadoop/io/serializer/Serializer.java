package org.apache.hadoop.io.serializer;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.IOException;
import java.io.OutputStream;

@InterfaceAudience.LimitedPrivate({"HDFS","MapReduce"})
@InterfaceStability.Evolving
public interface Serializer<T>{
    void open(OutputStream out) throws IOException;

    void serialize(T t)throws IOException;

    void close()throws IOException;
}
