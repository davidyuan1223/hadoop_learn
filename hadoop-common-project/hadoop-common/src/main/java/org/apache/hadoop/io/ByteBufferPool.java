package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.nio.ByteBuffer;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/23
 **/
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface ByteBufferPool {
    ByteBuffer getBuffer(boolean direct,int length);
    void putBuffer(ByteBuffer buffer);
    default void release(){}
}
