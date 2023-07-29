package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class ByteWritable implements WritableComparable<ByteWritable>{
    private byte value;
    public ByteWritable(){}
    public ByteWritable(byte value){
        set(value);
    }
    public void set(byte value){
        this.value=value;
    }
    public byte get(){
        return value;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        value=in.readByte();
    }

    @Override
    public void writer(DataOutput out) throws IOException {
        out.writeByte(value);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ByteWritable)) {
            return false;
        }
        ByteWritable other=(ByteWritable) o;
        return this.value== other.value;
    }

    @Override
    public int hashCode() {
        return value;
    }

    @Override
    public int compareTo(ByteWritable o) {
        int thisValue=this.value;
        int thatValue=o.value;
        return Integer.compare(thisValue,thatValue);
    }

    @Override
    public String toString() {
        return Byte.toString(value);
    }
    public static class Comparator extends WritableComparator{
        public Comparator(){super(ByteWritable.class);}

        @Override
        public int compare(byte[] b1, int start1, int length1, byte[] b2, int start2, int length2) {
            byte thisValue=b1[start1];
            byte thatValue=b2[start2];
            return Byte.compare(thisValue,thatValue);
        }
    }
    static {
        WritableComparator.define(ByteWritable.class,new Comparator());
    }
}
