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
public class FloatWritable implements WritableComparable<FloatWritable> {
    private float value;
    public FloatWritable(){}
    public FloatWritable(float value){
        set(value);
    }
    public void set(float value){
        this.value=value;
    }
    public float get(){
        return value;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        value=in.readFloat();
    }

    @Override
    public void writer(DataOutput out) throws IOException {
        out.writeFloat(value);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof FloatWritable)) {
            return false;
        }
        FloatWritable other=(FloatWritable)o;
        return this.value==other.value;
    }

    @Override
    public int hashCode() {
        return Float.floatToIntBits(value);
    }

    @Override
    public int compareTo(FloatWritable o) {
        return Float.compare(value,o.value);
    }

    @Override
    public String toString() {
        return Float.toString(value);
    }
    public static class Comparator extends WritableComparator{
        public Comparator(){
            super(FloatWritable.class);
        }

        @Override
        public int compare(byte[] b1, int start1, int length1, byte[] b2, int start2, int length2) {
            float thisValue=readFloat(b1,start1);
            float thatValue=readFloat(b2,start2);
            return (Float.compare(thisValue,thatValue));
        }
    }
    static {
        WritableComparator.define(FloatWritable.class,new Comparator());
    }
}
