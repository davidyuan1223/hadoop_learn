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
public class DoubleWritable implements WritableComparable<DoubleWritable> {
    private double value;
    public DoubleWritable(){}
    public DoubleWritable(double value){set(value);}
    public void set(double value){this.value=value;}
    public double get(){return value;}
    @Override
    public int compareTo(DoubleWritable o) {
        double thisValue=this.value;
        double thatValue=o.value;
        return (Double.compare(thisValue, thatValue));
    }

    @Override
    public void writer(DataOutput out) throws IOException {
        out.writeDouble(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        value=in.readDouble();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DoubleWritable)) {
            return false;
        }
        DoubleWritable other=(DoubleWritable)o;
        return this.value==other.value;
    }

    @Override
    public int hashCode() {
        return (int) value;
    }

    @Override
    public String toString() {
        return Double.toString(value);
    }

    public static class Comparator extends WritableComparator{
        public Comparator(){
            super(DoubleWritable.class);
        }

        @Override
        public int compare(byte[] b1, int start1, int length1, byte[] b2, int start2, int length2) {
            double thisValue = readDouble(b1, start1);
            double thatValue = readDouble(b2, start2);
            return (Double.compare(thisValue,thatValue));
        }
    }
    static {
        WritableComparator.define(DoubleWritable.class,new Comparator());
    }
}
