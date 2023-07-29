package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.*;

@InterfaceAudience.LimitedPrivate({"HDFS","MapReduce"})
@InterfaceStability.Unstable
public class OutputBuffer extends FilterOutputStream {
    private static class Buffer extends ByteArrayOutputStream{
        public byte[] getData(){return buf;}
        public int getLength(){return count;}

        @Override
        public  void reset() {
            count=0;
        }
        public void write(InputStream in,int len)throws IOException{
            int newCount=count+len;
            if (newCount > buf.length) {
                byte[] newBuf=new byte[Math.max(buf.length<<1,newCount)];
                System.arraycopy(buf,0,newBuf,0,count);
                buf=newBuf;
            }
            IOUtils.readFully(in,buf,count,len);
            count=newCount;
        }
    }
    private Buffer buffer;
    public OutputBuffer(){
        this(new Buffer());
    }
    private OutputBuffer(Buffer buffer){
        super(buffer);
        this.buffer=buffer;
    }
    public byte[] getData(){
        return buffer.getData();
    }
    public int getLength(){
        return buffer.getLength();
    }
    public OutputBuffer reset(){
        buffer.reset();
        return this;
    }
    public void write(InputStream in,int length) throws IOException {
        buffer.write(in,length);
    }
}
