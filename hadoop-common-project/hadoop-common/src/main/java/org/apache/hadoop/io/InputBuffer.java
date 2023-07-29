package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;

@InterfaceAudience.LimitedPrivate({"HDFS","MapReduce"})
@InterfaceStability.Unstable
public class InputBuffer extends FilterInputStream {
    private static class Buffer extends ByteArrayInputStream{
        public Buffer(){super(new byte[]{});}
        public void reset(byte[] input,int start,int length){
            this.buf=input;
            this.count=start+length;
            this.pos=start;
            this.mark=start;
        }
        public int getPosition(){return pos;}
        public int getLength(){return count;}
    }
    private Buffer buffer;
    public InputBuffer(){this(new Buffer());}
    public InputBuffer(Buffer buffer){
        super(buffer);
        this.buffer=buffer;
    }
    public void reset(byte[] input,int length){
        buffer.reset(input,0,length);
    }
    public void reset(byte[] input,int start,int length){
        buffer.reset(input,start,length);
    }
    public int getPosition(){
        return buffer.getPosition();
    }
    public int getLength(){
        return buffer.getLength();
    }
}
