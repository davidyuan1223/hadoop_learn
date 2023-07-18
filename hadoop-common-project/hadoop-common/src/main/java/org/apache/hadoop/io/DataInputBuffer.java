package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.InputStream;
@InterfaceAudience.LimitedPrivate({"HDFS","MapReduce"})
@InterfaceStability.Unstable
public class DataInputBuffer extends DataInputStream {
    private Buffer buffer;
    public DataInputBuffer(){this(new Buffer());}
    public DataInputBuffer(Buffer buffer){
        super(buffer);
        this.buffer=buffer;
    }
    public void reset(byte[] input,int length){buffer.reset(input,0,length);}
    public void reset(byte[] input,int start,int length){buffer.reset(input,start,length);}
    public byte[] getData(){return buffer.getData();}
    public int getPosition(){return buffer.getPosition();}


    private static class Buffer extends ByteArrayInputStream{
        public Buffer(){super(new byte[]{});}
        public void reset(byte[] input,int start,int length){
            this.buf=input;
            this.count=start+length;
            this.mark=start;
            this.pos=start;
        }
        public byte[] getData(){return buf;}
        public int getPosition(){return pos;}
        public int getLength(){return count;}

        @Override
        public int read() {
            return (pos<count)?(buf[pos++]&0xff):-1;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (b == null) {
                throw new NullPointerException();
            } else if (off < 0 || len < 0 || len > b.length - off) {
                throw new IndexOutOfBoundsException();
            }
            if (pos >= count) {
                return -1;
            }
            if (pos + len > count) {
                len=count-pos;
            }
            if (len == 0) {
                return 0;
            }
            System.arraycopy(buf,pos,b,off,len);
            pos+=len;
            return len;
        }

        @Override
        public long skip(long n) {
            if (pos + n > count) {
                n=count-pos;
            }
            if (n < 0) {
                return 0;
            }
            pos+=n;
            return n;
        }

        @Override
        public int available() {
            return count-pos;
        }
    }
}
