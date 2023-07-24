package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/23
 **/
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class VersionedWritable implements Writable {
    public abstract byte getVersion();

    @Override
    public void writer(DataOutput out) throws IOException {
        out.writeByte(getVersion());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        byte version = in.readByte();
        if (version!=getVersion()) {
            throw new VersionMismatchException(getVersion(),version);
        }
    }
}
