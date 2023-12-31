package org.apache.hadoop.fs.sftp;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class SFTPInputStream extends FSInputStream {
    private final ChannelSftp channel;
    private final Path path;
    private InputStream wrappedStream;
    private FileSystem.Statistics stats;
    private boolean closed;
    private long pos;
    private long nextPos;
    private long contentLength;
    SFTPInputStream(ChannelSftp channel,Path path,FileSystem.Statistics stats)throws IOException{
        try {
            this.channel=channel;
            this.path=path;
            this.stats=stats;
            this.wrappedStream=channel.get(path.toUri().getPath());
            SftpATTRS stat = channel.lstat(path.toString());
            this.contentLength=stat.getSize();
        }catch (SftpException e){
            throw new IOException(e);
        }
    }

    @Override
    public void seek(long pos) throws IOException {
        checkNotClosed();
        if (pos<0){
            throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK);
        }
        nextPos=pos;
    }

    @Override
    public int available() throws IOException {
        checkNotClosed();
        long remaining = contentLength - nextPos;
        if (remaining > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) remaining;
    }
    private void seekInternal()throws IOException{
        if (pos==nextPos) {
            return;
        }
        if (nextPos > pos) {
            long skipped = wrappedStream.skip(nextPos - pos);
            pos=pos+skipped;
        }
        if (nextPos < pos) {
            wrappedStream.close();
            try {
                wrappedStream=channel.get(path.toUri().getPath());
                pos=wrappedStream.skip(nextPos);
            }catch (SftpException e){
                throw new IOException(e);
            }
        }
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }

    @Override
    public synchronized long getPos() {
        return nextPos;
    }

    @Override
    public synchronized int read() throws IOException {
        checkNotClosed();
        if (this.contentLength == 0 || (nextPos >= contentLength)) {
            return -1;
        }
        seekInternal();
        int byteRead = wrappedStream.read();
        if (byteRead >= 0) {
            pos++;
            nextPos++;
        }
        if (stats != null & byteRead >= 0) {
            stats.incrementBytesRead(1);
        }
        return byteRead;
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        super.close();
        wrappedStream.close();
        closed=true;
    }
    private void checkNotClosed()throws IOException{
        if (closed) {
            throw new IOException(
                    path.toUri()+": "+FSExceptionMessages.STREAM_IS_CLOSED
            );
        }
    }
}
