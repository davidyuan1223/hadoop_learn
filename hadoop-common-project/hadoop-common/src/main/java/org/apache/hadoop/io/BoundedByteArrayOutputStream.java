package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
@InterfaceAudience.LimitedPrivate({"HDFS","MapReduce"})
@InterfaceStability.Unstable
public class BoundedByteArrayOutputStream extends OutputStream {
    private byte[] buffer;
    private int startOffset;
    private int limit;
    private int currentPointer;
    public BoundedByteArrayOutputStream(int capacity){
        this(capacity,capacity);
    }
    public BoundedByteArrayOutputStream(int capacity,int limit){
        this(new byte[capacity],0,limit);
    }
    private BoundedByteArrayOutputStream(byte[] buf,int offset,int limit){
        resetBuffer(buf,offset,limit);
    }
    protected void resetBuffer(byte[] buf,int offset,int limit){
        int capacity=buf.length-offset;
        if (capacity < limit || (capacity | limit) < 0) {
            throw new IllegalArgumentException("Invalid capacity/limit");
        }
        this.buffer=buf;
        this.startOffset=offset;
        this.limit=offset+limit;
        this.currentPointer=offset;
    }

    @Override
    public void write(int b) throws IOException {
        if (currentPointer>=limit) {
            throw new EOFException("Reaching the limit of the buffer");
        }
        buffer[currentPointer++]=(byte) b;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (off<0 || off>b.length || len<0 || off+len>b.length || off+len<0){
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }
        if (currentPointer + len > limit) {
            throw new EOFException("Reach the limit of the buffer");
        }
        System.arraycopy(b,off,buffer,currentPointer,len);
        currentPointer+=len;
    }
    public void reset(int newLim){
        if (newLim > (buffer.length - startOffset)) {
            throw new IndexOutOfBoundsException("Limit exceeds buffer size");
        }
        this.limit=newLim;
        this.currentPointer=startOffset;
    }
    public void reset(){
        this.limit=buffer.length-startOffset;
        this.currentPointer=startOffset;
    }
    public int getLimit(){
        return limit;
    }

    public byte[] getBuffer() {
        return buffer;
    }
    public int size(){
        return currentPointer-startOffset;
    }
    public int available(){
        return limit-currentPointer;
    }
}
