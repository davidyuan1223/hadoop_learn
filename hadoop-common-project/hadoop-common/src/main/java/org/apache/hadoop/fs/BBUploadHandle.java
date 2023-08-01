package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @Description: Byte array backed upload handle
 * @Author: yuan
 * @Date: 2023/07/30
 **/
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class BBUploadHandle implements UploadHandle{
    private static final long serialVersionUID = 0x69d5509b;
    private final byte[] bytes;
    private BBUploadHandle(ByteBuffer byteBuffer){
        this.bytes=byteBuffer.array();
    }
    public static BBUploadHandle from(ByteBuffer byteBuffer){
        return new BBUploadHandle(byteBuffer);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public ByteBuffer bytes() {
        return ByteBuffer.wrap(bytes);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof BBUploadHandle)) {
            return false;
        }
        UploadHandle other = (UploadHandle) obj;
        return bytes().equals(other.bytes());
    }
}
