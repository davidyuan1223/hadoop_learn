package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.IntFunction;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface PositionedReadable {
    int read(long position,byte[] buffer,int offset,int length)throws IOException;
    void readFully(long position,byte[] buffer,int offset,int length)throws IOException;
    void readFully(long position, byte[] buffer)throws IOException;
    default int minSeekForVectorReads(){
        return 4*1024;
    }
    default int maxReadSizeForVectorReads(){
        return 1024*1024;
    }
    default void readVectored(List<? extends FileRange> range, IntFunction<ByteBuffer> allocate)throws IOException{
        VectoredReadUtils.readVectored(this,range,allocate);
    }
}
