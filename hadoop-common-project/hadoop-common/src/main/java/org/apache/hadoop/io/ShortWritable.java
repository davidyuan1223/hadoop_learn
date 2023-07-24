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
public class ShortWritable implements WritableComparable<ShortWritable> {
    private short value;
    public ShortWritable(){}
    public ShortWritable(short value){set(value);}
    public void set(short value){this.value=value;}
    public short get(){return value;}
    @Override
    public int compareTo(ShortWritable o) {
        short thisValue=this.value;
        short thatValue=o.value;
        return (Short.compare(thisValue, thatValue));
    }

    @Override
    public void writer(DataOutput out) throws IOException {
        out.writeInt(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        value=in.readShort();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ShortWritable)) {
            return false;
        }
        ShortWritable other=(ShortWritable)o;
        return this.value==other.value;
    }

    @Override
    public int hashCode() {
        return value;
    }

    @Override
    public String toString() {
        return Short.toString(value);
    }

    public static class Comparator extends WritableComparator{
        public Comparator(){
            super(ShortWritable.class);
        }

        @Override
        public int compare(byte[] b1, int start1, int length1, byte[] b2, int start2, int length2) {
            short thisValue = (short) readUnsignedShort(b1, start1);
            short thatValue = (short) readUnsignedShort(b2, start2);
            return (Short.compare(thisValue,thatValue));
        }
    }
    static {
        WritableComparator.define(ShortWritable.class,new Comparator());
    }
}
