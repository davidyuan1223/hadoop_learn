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
public class LongWritable implements WritableComparable<LongWritable> {
    private long value;
    public LongWritable(){}
    public LongWritable(long value){set(value);}
    public void set(long value){this.value=value;}
    public long get(){return value;}
    @Override
    public int compareTo(LongWritable o) {
        long thisValue=this.value;
        long thatValue=o.value;
        return (Long.compare(thisValue, thatValue));
    }

    @Override
    public void writer(DataOutput out) throws IOException {
        out.writeLong(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        value=in.readLong();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof LongWritable)) {
            return false;
        }
        LongWritable other=(LongWritable)o;
        return this.value==other.value;
    }

    @Override
    public int hashCode() {
        return (int) value;
    }

    @Override
    public String toString() {
        return Long.toString(value);
    }

    public static class Comparator extends WritableComparator{
        public Comparator(){
            super(LongWritable.class);
        }

        @Override
        public int compare(byte[] b1, int start1, int length1, byte[] b2, int start2, int length2) {
            long thisValue = readLong(b1, start1);
            long thatValue = readLong(b2, start2);
            return (Long.compare(thisValue,thatValue));
        }
    }
    static {
        WritableComparator.define(LongWritable.class,new Comparator());
    }
}
