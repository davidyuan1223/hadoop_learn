package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.ByteBufferPool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface HasEnhanceByteBufferAccess {
    ByteBuffer read(ByteBufferPool factory, int maxLength, EnumSet<ReadOption> opts)throws IOException,UnsupportedOperationException;
    void releaseBuffer(ByteBuffer buffer);
}
