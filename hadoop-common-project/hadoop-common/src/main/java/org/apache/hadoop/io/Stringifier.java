package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.Closeable;
import java.io.IOException;

@InterfaceAudience.Public
@InterfaceStability.Stable
public interface Stringifier<T> extends Closeable {
    String toString(T obj)throws IOException;

    T fromString(String str)throws IOException;

    @Override
    void close() throws IOException;
}
