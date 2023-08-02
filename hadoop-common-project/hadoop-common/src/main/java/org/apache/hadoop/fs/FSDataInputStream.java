package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.impl.StoreImplementationUtils;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.IOStatisticsSupport;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.util.IdentityHashStore;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.EnumSet;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class FSDataInputStream extends DataInputStream
    implements Seekable,PositionedReadable,ByteBufferReadable,
    HasFileDescriptor,CanSetDropBehind,CanSetReadahead,HasEnhanceByteBufferAccess,
    CanUnbuffer,StreamCapabilities,ByteBufferPositionedReadable, IOStatisticsSource
{
    private final IdentityHashStore<ByteBuffer, ByteBufferPool> extendedReadBuffers=new IdentityHashStore<>(0);
    public FSDataInputStream(InputStream in){
        super(in);
        if (!(in instanceof Seekable) || !(in instanceof PositionedReadable)) {
            throw new IllegalArgumentException(in.getClass().getCanonicalName()+
                    " is not an instance of Seekable or PositionedReadable");
        }
    }

    @Override
    public void seek(long pos) throws IOException {
        ((Seekable)in).seek(pos);
    }

    @Override
    public long getPos() throws IOException {
        return ((Seekable)in).getPos();
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
        return ((PositionedReadable)in).read(position,buffer,offset,length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        ((PositionedReadable)in).readFully(position,buffer,offset,length);
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
        ((PositionedReadable)in).readFully(position,buffer);
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return ((Seekable)in).seekToNewSource(targetPos);
    }

    @InterfaceAudience.Public
    @InterfaceStability.Stable
    public InputStream getWrappedStream(){
        return in;
    }

    @Override
    public int read(ByteBuffer buf) throws IOException {
        if (in instanceof ByteBufferReadable) {
            return ((ByteBufferReadable)in).read(buf);
        }
        throw new UnsupportedOperationException("Byte-buffer read unsupported by "+in.getClass().getCanonicalName());
    }

    @Override
    public FileDescriptor getFileDescriptor() throws IOException {
        if (in instanceof HasFileDescriptor) {
            return ((HasFileDescriptor)in).getFileDescriptor();
        } else if (in instanceof FileInputStream) {
            return ((FileInputStream)in).getFD();
        }else {
            return null;
        }
    }

    @Override
    public void setReadahead(Long readahead) throws IOException, UnsupportedOperationException {
        try {
            ((CanSetReadahead)in).setReadahead(readahead);
        }catch (ClassCastException e){
            throw new UnsupportedOperationException(in.getClass().getCanonicalName()
            +" does not support setting the readahead caching strategy");
        }
    }

    @Override
    public void setDropBehind(Boolean dropCache) throws IOException, UnsupportedOperationException {
        try {
            ((CanSetDropBehind)in).setDropBehind(dropCache);
        }catch (ClassCastException e){
            throw new UnsupportedOperationException("this stream does not support setting the drop-behind caching setting.");
        }
    }

    @Override
    public ByteBuffer read(ByteBufferPool factory, int maxLength, EnumSet<ReadOption> opts) throws IOException, UnsupportedOperationException {
        try {
            return ((HasEnhanceByteBufferAccess)in).read(factory,maxLength,opts);
        }catch (ClassCastException e){
            ByteBuffer buffer=ByteBufferUtil.fallbackRead(this,factory,maxLength);
            if (buffer != null) {
                extendedReadBuffers.put(buffer,factory);
            }
            return buffer;
        }
    }

    private static final EnumSet<ReadOption> EMPTY_READ_OPTIONS_SET=EnumSet.noneOf(ReadOption.class);

    final public ByteBuffer read(ByteBufferPool factory,int maxLength)throws IOException,UnsupportedOperationException{
        return read(factory,maxLength,EMPTY_READ_OPTIONS_SET);
    }

    @Override
    public void releaseBuffer(ByteBuffer buffer) {
        try {
            ((HasEnhanceByteBufferAccess)in).releaseBuffer(buffer);
        }catch (ClassCastException e){
            ByteBufferPool bufferPool = extendedReadBuffers.remove(buffer);
            if (bufferPool == null) {
                throw new IllegalArgumentException("tried to release a buffer that was not created by this stream.");
            }
            bufferPool.putBuffer(buffer);
        }
    }

    @Override
    public void unbuffer() {
        StreamCapabilitiesPolicy.unbuffer(in);
    }

    @Override
    public boolean hasCapability(String capability) {
        return StoreImplementationUtils.hasCapability(in,capability);
    }

    @Override
    public String toString() {
        return super.toString()+":"+in;
    }

    @Override
    public int read(long position, ByteBuffer buf) throws IOException {
        if (in instanceof ByteBufferPositionedReadable) {
            return ((ByteBufferPositionedReadable)in).read(position,buf);
        }
        throw new UnsupportedEncodingException("Byte-buffer pread unsupported " +
                "by "+in.getClass().getCanonicalName());
    }

    @Override
    public void readFully(long position, ByteBuffer buf) throws IOException {
        if (in instanceof ByteBufferPositionedReadable) {
            ((ByteBufferPositionedReadable)in).readFully(position,buf);
        }else {
            throw new UnsupportedEncodingException("Byte-buffer pread unsupported by "+in.getClass().getCanonicalName());
        }
    }

    @Override
    public IOStatistics getIOStatistics() {
        return IOStatisticsSupport.retrieveIOStatistics(in);
    }
}
