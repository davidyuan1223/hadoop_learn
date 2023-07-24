package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/23
 **/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class BooleanWritable implements WritableComparable<BooleanWritable> {
    private boolean value;
    public BooleanWritable(){}
    public BooleanWritable(boolean value){set(value);}
    public void set(boolean value){this.value=value;}
    public boolean get(){return value;}

    @Override
    public void readFields(DataInput in) throws IOException {
        value=in.readBoolean();
    }

    @Override
    public void writer(DataOutput out) throws IOException {
        out.writeBoolean(value);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof BooleanWritable)) {
            return false;
        }
        BooleanWritable other=(BooleanWritable) o;
        return this.value==other.value;
    }

    @Override
    public int hashCode() {
        return value?0:1;
    }

    @Override
    public int compareTo(BooleanWritable o) {
        return Boolean.compare(this.value,o.value);
    }

    @Override
    public String toString() {
        return Boolean.toString(get());
    }
    public static class Comparator extends WritableComparator{
        public Comparator(){super(BooleanWritable.class);}

        @Override
        public int compare(byte[] b1, int start1, int length1, byte[] b2, int start2, int length2) {
            return compareBytes(b1, start1, length1, b2, start2, length2);
        }
    }
    static {
        WritableComparator.define(BooleanWritable.class,new Comparator());
    }
}
