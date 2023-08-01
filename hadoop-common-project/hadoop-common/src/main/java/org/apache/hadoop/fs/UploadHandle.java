package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Opaque,serializable reference to an uploadId for multiple uploads
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface UploadHandle extends Serializable {
    default byte[] toByteArray() {
        ByteBuffer bb = bytes();
        byte[] bytes = new byte[bb.remaining()];
        bb.get(bytes);
        return bytes;
    }
    ByteBuffer bytes();
    @Override
    boolean equals(Object other);
}
