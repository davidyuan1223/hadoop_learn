package org.apache.hadoop.fs.http;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLConnection;

public abstract class AbstractHttpFileSystem extends FileSystem {
    private static final long DEFAULT_BLOCK_SIZE=4096;
    private static final Path WORKING_DIR=new Path("/");
    private URI uri;
    @Override
    public void initialize(URI name, Configuration conf)throws IOException{
        super.initialize(name,conf);
        this.uri=name;
    }
    public abstract String getScheme();
    @Override
    public URI getUri(){
        return uri;
    }
    @Override
    public FSDataInputStream open(Path path,int bufferSize)throws IOException{
        URI pathUri=makeQualified(path).toUri();
        URLConnection conn = pathUri.toURL().openConnection();
        InputStream in = conn.getInputStream();
        return new FSDataInputStream(new HttpDataInputStream(in));
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean b, int i, short i1, long l, Progressable progressable)throws IOException{
        throw new UnsupportedEncodingException();
    }
    @Override
    public FSDataOutputStream append(Path path,int i,Progressable progressable)throws IOException{
        throw new UnsupportedEncodingException();
    }
    @Override
    public boolean rename(Path src,Path dest)throws IOException{
        throw new UnsupportedEncodingException();
    }
    @Override
    public boolean delete(Path path,boolean b)throws IOException{
        throw new UnsupportedEncodingException();
    }
    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setWorkingDirectory(Path path) {
    }

    @Override
    public Path getWorkingDirectory() {
        return WORKING_DIR;
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission)
            throws IOException {
        return false;
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        return new FileStatus(-1, false, 1, DEFAULT_BLOCK_SIZE, 0, makeQualified(path));
    }

    @Override
    public boolean hasPathCapability(final Path path, final String capability)
            throws IOException {
        switch (validatePathCapabilityArgs(path, capability)) {
            case CommonPathCapabilities.FS_READ_ONLY_CONNECTOR:
                return true;
            default:
                return super.hasPathCapability(path, capability);
        }
    }
    private static class HttpDataInputStream extends FilterInputStream implements Seekable, PositionedReadable{
        HttpDataInputStream(InputStream in){
            super(in);
        }

        @Override
        public int read(long position, byte[] buffer, int offset, int length) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void readFully(long position, byte[] buffer) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void seek(long pos) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getPos() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean seekToNewSource(long targetPos) throws IOException {
            throw new UnsupportedEncodingException();
        }
    }
}
