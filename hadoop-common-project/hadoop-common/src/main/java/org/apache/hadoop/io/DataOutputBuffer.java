package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import java.io.*;

@InterfaceAudience.LimitedPrivate({"HDFS","MapReduce"})
@InterfaceStability.Unstable
public class DataOutputBuffer extends DataOutputStream {
    private static class Buffer extends ByteArrayOutputStream{
        public byte[] getData(){return buf;}
        public int getLength(){return count;}
        public Buffer(){super();}
        public Buffer(int size){super(size);}
        public void write(DataInput in,int len)throws IOException{
            int newCount=count+len;
            if (newCount > buf.length) {
                byte[] newBuf=new byte[Math.max(buf.length<<1,newCount)];
                System.arraycopy(buf,0,newBuf,0,count);
                buf=newBuf;
            }
            in.readFully(buf,count,len);
            count=newCount;
        }
        private int setCount(int newCount){
            Preconditions.checkArgument(newCount>=0 && newCount<=buf.length);
            int oldCount=count;
            count=newCount;
            return oldCount;
        }
    }
    private Buffer buffer;
    public DataOutputBuffer(){
        this(new Buffer());
    }
    public DataOutputBuffer(int size){
        this(new Buffer(size));
    }
    public DataOutputBuffer(Buffer buffer){
        super(buffer);
        this.buffer=buffer;
    }

    public byte[] getData(){
        return buffer.getData();
    }
    public int getLength(){
        return buffer.getLength();
    }
    public DataOutputBuffer reset(){
        this.written=0;
        buffer.reset();
        return this;
    }
    public void write(DataInput in,int length)throws IOException{
        buffer.write(in,length);
    }
    public void writeTo(OutputStream out)throws IOException{
        buffer.writeTo(out);
    }
    public void writeInt(int v,int offset)throws IOException{
        Preconditions.checkState(offset + 4 <= buffer.getLength());
        byte[] b = new byte[4];
        b[0] = (byte) ((v >>> 24) & 0xFF);
        b[1] = (byte) ((v >>> 16) & 0xFF);
        b[2] = (byte) ((v >>> 8) & 0xFF);
        b[3] = (byte) ((v >>> 0) & 0xFF);
        int oldCount = buffer.setCount(offset);
        buffer.write(b);
        buffer.setCount(oldCount);
    }

}
