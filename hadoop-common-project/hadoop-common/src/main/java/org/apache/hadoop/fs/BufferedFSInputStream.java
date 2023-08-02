package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.IOStatisticsSupport;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.FileDescriptor;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.StringJoiner;
import java.util.function.IntFunction;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class BufferedFSInputStream  extends BufferedInputStream
implements Seekable,PositionedReadable, HasFileDescriptor, IOStatisticsSource,StreamCapabilities{

    public BufferedFSInputStream(FSInputStream in,int size){
        super(in,size);
    }

    @Override
    public long getPos() throws IOException {
        if (in == null) {
            throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
        }
        return ((FSInputStream)in).getPos()-(count-pos);
    }

    @Override
    public synchronized long skip(long n) throws IOException {
        if (n <= 0) {
            return 0;
        }
        seek(getPos()+n);
        return n;
    }

    @Override
    public void seek(long pos) throws IOException {
        if (in == null) {
            throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
        }
        if (pos < 0) {
            throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK);
        }
        if (this.pos != this.count) {
            long end=((FSInputStream)in).getPos();
            long start=end-count;
            if (pos >= start && pos < end) {
                this.pos= (int) (pos-start);
                return;
            }
        }
        this.pos=0;
        this.count=0;
        ((FSInputStream)in).seek(pos);
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        pos=0;
        count=0;
        return ((FSInputStream)in).seekToNewSource(targetPos);
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
        return ((FSInputStream)in).read(position,buffer,offset,length);
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
        ((FSInputStream)in).readFully(position,buffer);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        ((FSInputStream)in).readFully(position,buffer,offset,length);
    }

    @Override
    public FileDescriptor getFileDescriptor() throws IOException {
        if (in instanceof HasFileDescriptor) {
            return ((HasFileDescriptor)in).getFileDescriptor();
        }
        return null;
    }

    @Override
    public boolean hasCapability(String capability) {
        if (in instanceof StreamCapabilities) {
            return ((StreamCapabilities)in).hasCapability(capability);
        }
        return false;
    }

    @Override
    public IOStatistics getIOStatistics() {
        return IOStatisticsSupport.retrieveIOStatistics(in);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ",
                BufferedFSInputStream.class.getSimpleName()+"[","]")
                .add("in="+in)
                .toString();
    }

    @Override
    public int minSeekForVectorReads() {
        return ((PositionedReadable)in).minSeekForVectorReads();
    }

    @Override
    public int maxReadSizeForVectorReads() {
        return ((PositionedReadable)in).maxReadSizeForVectorReads();
    }

    @Override
    public void readVectored(List<? extends FileRange> range, IntFunction<ByteBuffer> allocate) throws IOException {
        ((PositionedReadable)in).readVectored(range,allocate);
    }
}
