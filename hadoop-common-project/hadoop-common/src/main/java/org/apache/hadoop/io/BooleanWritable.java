package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class BooleanWritable implements WritableComparable<BooleanWritable>{
    private boolean value;
    public BooleanWritable(){}
    public BooleanWritable(boolean value){this.value=value;}
    public void set(boolean value){this.value=value;}
    public boolean get(){return value;}
    @Override
    public int compareTo(BooleanWritable o) {
        boolean a=this.value;
        boolean b=o.value;
        return (a==b) ? 0:(!a)?-1:1;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeBoolean(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        value=in.readBoolean();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof BooleanWritable)) {
            return false;
        }
        BooleanWritable other= (BooleanWritable) o;
        return this.value==other.value;
    }

    @Override
    public int hashCode() {
        return value?0:1;
    }

    static {

    }
}
