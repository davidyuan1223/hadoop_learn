package org.apache.hadoop.io.serializer;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.IOException;
import java.io.InputStream;

@InterfaceAudience.LimitedPrivate({"HDFS","MapReduce"})
@InterfaceStability.Evolving
public interface DeSerializer<T>{
    void open(InputStream in)throws IOException;

    T deserialize(T t)throws IOException;

    void close() throws IOException;
}
