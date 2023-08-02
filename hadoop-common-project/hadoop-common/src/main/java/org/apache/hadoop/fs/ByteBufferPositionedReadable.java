package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.IOException;
import java.nio.ByteBuffer;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface ByteBufferPositionedReadable {
    int read(long position, ByteBuffer buf)throws IOException;
    void readFully(long position,ByteBuffer buf)throws IOException;
}
