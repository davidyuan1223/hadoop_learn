package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class ByteBufferUtil {
    private static boolean streamHasByteBufferRead(InputStream stream){
        if (!(stream instanceof ByteBufferReadable)) {
            return false;
        }
        if (!(stream instanceof FSDataInputStream)) {
            return true;
        }
        return ((FSDataInputStream)stream).getWrappedStream() instanceof ByteBufferReadable;
    }

    public static ByteBuffer fallbackRead(InputStream stream, ByteBufferPool bufferPool,int maxLength)throws IOException{
        if (bufferPool == null) {
            throw new UnsupportedOperationException("zero-copy reads were not available, and you did not provide a fallback ByteBufferPool.");
        }
        boolean useDirect=streamHasByteBufferRead(stream);
        ByteBuffer buffer = bufferPool.getBuffer(useDirect, maxLength);
        if (buffer == null) {
            throw new UnsupportedOperationException("zero-copy reads were not available, and the ByteBufferPool did not provide" +
                    " us with "+(useDirect?"a direct":"an indirect")+"buffer.");
        }
        Preconditions.checkState(buffer.capacity()>0);
        Preconditions.checkState(buffer.isDirect()==useDirect);
        maxLength=Math.min(maxLength,buffer.capacity());
        boolean success=false;
        try {
            if (useDirect) {
                buffer.clear();
                buffer.limit(maxLength);
                ByteBufferReadable readable= (ByteBufferReadable) stream;
                int totalRead=0;
                while (true) {
                    if (totalRead >= maxLength) {
                        success=true;
                        break;
                    }
                    int nRead=readable.read(buffer);
                    if (nRead < 0) {
                        if (totalRead > 0) {
                            success=true;
                        }
                        break;
                    }
                    totalRead+=nRead;
                }
                buffer.flip();
            }else {
                buffer.clear();
                int nRead=stream.read(buffer.array(),buffer.arrayOffset(),maxLength);
                if (nRead >= 0) {
                    buffer.limit(nRead);
                    success=true;
                }
            }
        }finally {
            if (!success) {
                bufferPool.putBuffer(buffer);
                buffer=null;
            }
        }
        return buffer;
    }
}
