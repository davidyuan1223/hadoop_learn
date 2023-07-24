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
public class NullWritable implements WritableComparable<NullWritable> {
    private static final NullWritable  THIS=new NullWritable();
    private NullWritable(){}
    public static NullWritable get(){return THIS;}

    @Override
    public String toString() {
        return "(null)";
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public int compareTo(NullWritable o) {
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof NullWritable;
    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }

    @Override
    public void writer(DataOutput out) throws IOException {

    }
    public static class Comparator extends WritableComparator{
        public Comparator(){super(NullWritable.class);}

        @Override
        public int compare(byte[] b1, int start1, int length1, byte[] b2, int start2, int length2) {
            assert 0==length1;
            assert 0==length2;
            return 0;
        }
    }
    static {
        WritableComparator.define(NullWritable.class,new Comparator());
    }
}
