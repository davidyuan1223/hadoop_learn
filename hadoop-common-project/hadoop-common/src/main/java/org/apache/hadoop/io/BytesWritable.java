package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class BytesWritable extends BinaryComparable implements WritableComparable<BinaryComparable>{
    private static final int MAX_ARRAY_SIZE=Integer.MAX_VALUE-8;
    private static final int LENGTH_BYTES=4;
    private static final byte[] EMPTY_BYTES=new byte[0];
    private int size;
    private byte[] bytes;
    public BytesWritable(){
        this.bytes=EMPTY_BYTES;
        this.size=0;
    }
    public BytesWritable(byte[] bytes){
        this(bytes,bytes.length);
    }
    public BytesWritable(byte[] bytes,int length){
        this.bytes=bytes;
        this.size=length;
    }
    public byte[] copyBytes(){
        return Arrays.copyOf(bytes,size);
    }

    @Override
    public byte[] getBytes() {
        return bytes;
    }
    @Deprecated
    public byte[] get(){
        return getBytes();
    }

    @Override
    public int getLength() {
        return size;
    }
    @Deprecated
    public int getSize(){return getLength();}
    public void setSize(int size){
        if (size>getCapacity()){
            long newSize=Math.min(MAX_ARRAY_SIZE,(3L*size)/2L);
            setCapacity((int)newSize);
        }
        this.size=size;
    }
    public int getCapacity(){return bytes.length;}
    public void setCapacity(final int capacity){
        if (capacity != getCapacity()) {
            this.size=Math.min(size,capacity);
            this.bytes=Arrays.copyOf(this.bytes,capacity);
        }
    }
    public void set(BytesWritable newData){
        set(newData.bytes,0,newData.size);
    }
    public void set(byte[] newData,int offset,int length){
        setSize(0);
        setSize(length);
        System.arraycopy(newData,offset,bytes,0,size);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        setSize(0);
        setSize(in.readInt());
        in.readFully(bytes,0,size);
    }

    @Override
    public void writer(DataOutput out) throws IOException {
        out.writeInt(size);
        out.write(bytes,0,size);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof BytesWritable) {
            return super.equals(o);
        }
        return false;
    }


    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public String toString() {
        return IntStream.range(0,size)
                .mapToObj(idx->String.format("%02x",bytes[idx]))
                .collect(Collectors.joining(" "));
    }
    public static class Comparator extends WritableComparator{
        public Comparator(){super(BytesWritable.class);}

        @Override
        public int compare(byte[] b1, int start1, int length1, byte[] b2, int start2, int length2) {
            return compareBytes(b1,start1+LENGTH_BYTES,length1-LENGTH_BYTES,
                    b2,start2+LENGTH_BYTES,length2-LENGTH_BYTES);
        }
    }
    static {
        WritableComparator.define(BytesWritable.class,new UTF8.Comparator());
    }

}
