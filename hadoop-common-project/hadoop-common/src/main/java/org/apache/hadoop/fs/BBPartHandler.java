package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Byte array backed part handler.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class BBPartHandler implements PartHandler{
    private static final long serialVersionUID=0x23ce3eb1;
    private final byte[] bytes;
    private BBPartHandler(ByteBuffer byteBuffer){
        this.bytes=byteBuffer.array();
    }
    public static PartHandler from(ByteBuffer byteBuffer){
        return new BBPartHandler(byteBuffer);
    }

    @Override
    public ByteBuffer bytes() {
        return ByteBuffer.wrap(bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof BBPartHandler)) {
            return false;
        }
        PartHandler that = (PartHandler)other;
        return bytes().equals(that.bytes());
    }
}

