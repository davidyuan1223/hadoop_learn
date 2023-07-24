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
public class IntWritable implements WritableComparable<IntWritable> {
    private int value;
    public IntWritable(){}
    public IntWritable(int value){set(value);}
    public void set(int value){this.value=value;}
    public int get(){return value;}
    @Override
    public int compareTo(IntWritable o) {
        int thisValue=this.value;
        int thatValue=o.value;
        return (Integer.compare(thisValue, thatValue));
    }

    @Override
    public void writer(DataOutput out) throws IOException {
        out.writeInt(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        value=in.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof IntWritable)) {
            return false;
        }
        IntWritable other=(IntWritable)o;
        return this.value==other.value;
    }

    @Override
    public int hashCode() {
        return value;
    }

    @Override
    public String toString() {
        return Integer.toString(value);
    }

    public static class Comparator extends WritableComparator{
        public Comparator(){
            super(IntWritable.class);
        }

        @Override
        public int compare(byte[] b1, int start1, int length1, byte[] b2, int start2, int length2) {
            int thisValue = readInt(b1, start1);
            int thatValue = readInt(b2, start2);
            return (Integer.compare(thisValue,thatValue));
        }
    }
    static {
        WritableComparator.define(IntWritable.class,new Comparator());
    }
}
