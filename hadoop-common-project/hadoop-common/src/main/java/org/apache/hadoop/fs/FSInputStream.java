package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class FSInputStream extends InputStream implements Seekable,PositionedReadable{
    private static final Logger LOG= LoggerFactory.getLogger(FSInputStream.class);

    @Override
    public abstract void seek(long pos) throws IOException;

    @Override
    public abstract long getPos() throws IOException;

    @Override
    public abstract boolean seekToNewSource(long targetPos) throws IOException;

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
        validatePositionedReadArgs(position,buffer,offset,length);
        if (length==0) {
            return 0;
        }
        synchronized (this){
            long oldPos=getPos();
            int nread=-1;
            try {
                seek(position);
                nread=read(buffer,offset,length);
            }catch (EOFException e){
                LOG.debug("Downgrading EOFException raised trying to read {} bytes at offset {}",length,offset,e);
            }finally {
                seek(oldPos);
            }
            return nread;
        }
    }

    protected void validatePositionedReadArgs(long position,byte[] buffer,int offset,int length)throws EOFException{
        Preconditions.checkArgument(length>=0,"length is negative");
        if (position < 0) {
            throw new EOFException("position is negative");
        }
        Preconditions.checkArgument(buffer!=null,"Null buffer");
        if (buffer.length - offset < length) {
            throw new IndexOutOfBoundsException(
                    FSExceptionMessages.TOO_MANY_BYTES_FOR_DEST_BUFFER
                    +": request length="+length
                    +", with offset ="+offset
                    +"; buffer capacity ="+(buffer.length-offset)
            );
        }
    }


    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        validatePositionedReadArgs(position,buffer,offset,length);
        int nread=0;
        while (nread < length) {
            int nbytes=read(position+nread,buffer,offset+nread,length-nread);
            if (nbytes < 0) {
                throw new EOFException(FSExceptionMessages.EOF_IN_READ_FULLY);
            }
            nread+=nbytes;
        }
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
        readFully(position,buffer,0,buffer.length);
    }

    @Override
    public String toString() {
        final StringBuilder sb=new StringBuilder(super.toString());
        sb.append('{');
        if (this instanceof IOStatisticsSource) {
            sb.append(IOStatisticsLogging.ioStatisticsSourceToString(this));
        }
        sb.append('}');
        return sb.toString();
    }
}
