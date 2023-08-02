package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.InputStream;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class ByteBufferUtil {
    private static boolean streamHasByteBufferRead(InputStream stream){
        if (!(stream instanceof ByteBufferReadable)) {
            return false;
        }
        if (!(stream instanceof FSDataInputStream)) {

        }
    }
}
