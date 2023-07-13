package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@InterfaceAudience.Public
@InterfaceStability.Stable
public interface Writable {
    void write(DataOutput out)throws IOException;
    void readFields(DataInput input)throws IOException;
}
