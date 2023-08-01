package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/30
 **/
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface PartHandle extends Serializable {
    default byte[] toByteArray(){
        ByteBuffer bb=bytes();
        byte[] ret=new byte[bb.remaining()];
        bb.get(ret);
        return ret;
    }
    ByteBuffer bytes();
    @Override
    boolean equals(Object other);
}
