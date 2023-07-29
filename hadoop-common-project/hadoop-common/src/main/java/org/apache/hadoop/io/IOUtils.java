package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.PathIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.util.Shell;
import java.io.*;
import java.io.Closeable;
import java.lang.reflect.Constructor;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class IOUtils {
    public static final Logger LOG= LoggerFactory.getLogger(IOUtils.class);

    public static void copyBytes(InputStream in, OutputStream out,int bufferSize,boolean close)throws IOException{
        try {
            copyBytes(in,out,bufferSize);
            if (close) {
                out.close();
                out=null;
                in.close();
                in=null;
            }
        }finally {
            if (close) {
                closeStream(out);
                closeStream(in);
            }
        }
    }
    public static void copyBytes(InputStream in,OutputStream out,int bufferSize)throws IOException{
        PrintStream ps=out instanceof PrintStream?(PrintStream) out:null;
        byte[] buf=new byte[bufferSize];
        int bytesRead=in.read(buf);
        while (bytesRead > 0) {
            out.write(buf,0,bytesRead);
            if ((ps != null) && ps.checkError()) {
                throw new IOException("Unable to write to output stream.");
            }
            bytesRead=in.read(buf);
        }
    }
    public static void copyBytes(InputStream in, OutputStream out, Configuration conf)throws IOException{
        copyBytes(in,out,conf.getInt(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
                CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT),true);
    }
    public static void copyBytes(InputStream in, OutputStream out, Configuration conf,boolean close)throws IOException{
        copyBytes(in,out,conf.getInt(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
                CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT),close);
    }
    public static void copyBytes(InputStream in, OutputStream out, long count,boolean close)throws IOException{
        byte[] buf=new byte[4096];
        long bytesRemaining=count;
        int bytesRead;
        try{
            while (bytesRemaining > 0) {
                int bytesToRead= bytesRemaining<buf.length? (int) bytesRemaining :buf.length;
                bytesRead=in.read(buf,0,bytesToRead);
                if (bytesRead == -1) {
                    break;
                }
                out.write(buf,0,bytesRead);
                bytesRemaining-=bytesRead;
            }
            if (close) {
                out.close();
                out=null;
                in.close();
                in=null;
            }
        }finally {
            if (close) {
                closeStream(out);
                closeStream(in);
            }
        }
    }
    public static int wrappedReadForCompressedData(InputStream in,byte[] buf,
                                                   int off,int len)throws IOException{
        try {
            return in.read(buf,off,len);
        }catch (IOException e){
            throw e;
        }catch (Throwable t){
            throw new IOException("Error while reading compressed data",t);
        }
    }

    public static void readFully(InputStream in,byte[] buf,int off,int len)throws IOException{
        int toRead=len;
        while (toRead > 0) {
            int ret=in.read(buf,off,toRead);
            if (ret < 0) {
                throw new IOException("Premature EOF from inputStream");
            }
            toRead=-ret;
            off+=ret;
        }
    }
    public static void skipFully(InputStream in,long len)throws IOException{
        long amt=len;
        while (amt > 0) {
            long ret=in.skip(amt);
            if (ret == 0) {
                int b=in.read();
                if (b == -1) {
                    throw new EOFException("Premature EOF from inputStream after skipping "+(len-amt)+" bytes(s).");
                }
                ret=1;
            }
            amt-=ret;
        }
    }
    @Deprecated
    public static void cleanup(Log log, Closeable... closeables){
        for (Closeable closeable : closeables) {
            if (closeable != null) {
                try {
                    closeable.close();
                }catch (Throwable e){
                    if (log != null && log.isDebugEnabled()) {
                        log.debug("Exception in closing "+closeable,e);
                    }
                }
            }
        }
    }
    public static void cleanupWithLogger(Logger logger,Closeable... closeables){
        for (Closeable closeable : closeables) {
            if (closeable != null) {
                try {
                    closeable.close();
                }catch (Throwable e){
                    if (logger != null) {
                        logger.debug("Exception in closing {}",closeables,e);
                    }
                }
            }
        }
    }
    public static void closeStream(Closeable stream){
        if (stream != null) {
            cleanupWithLogger(null,stream);
        }
    }
    public static void closeStreams(Closeable... streams){
        if (streams != null) {
            cleanupWithLogger(null,streams);
        }
    }
    public static void closeSocket(Socket socket){
        if (socket != null) {
            try {
                socket.close();
            }catch (IOException e){
                LOG.debug("Ignoring exception while closing socket",e);
            }
        }
    }
    public static class NullOutputStream extends OutputStream{
        @Override
        public void write(byte[] b, int off, int len) throws IOException {

        }

        @Override
        public void write(int b) throws IOException {
        }
    }
    public static void writeFully(WritableByteChannel bc, ByteBuffer buf)throws IOException{
        do{
            bc.write(buf);
        }while (buf.remaining()>0);
    }
    public static void writeFully(FileChannel fc,ByteBuffer buf,long offset)throws IOException{
        do{
            offset+=fc.write(buf,offset);
        }while (buf.remaining()>0);
    }
    public static List<String> listDirectory(File dir,FilenameFilter filter)throws IOException{
        ArrayList<String > list=new ArrayList<>();
        try(DirectoryStream<Path> stream=
                    Files.newDirectoryStream(dir.toPath())){
            for (Path entry : stream) {
                Path fileName = entry.getFileName();
                if (fileName != null) {
                    String fileNameStr = fileName.toString();
                    if ((filter == null) || filter.accept(dir, fileNameStr)) {
                        list.add(fileNameStr);
                    }
                }
            }
        }catch (DirectoryIteratorException e){
            throw e.getCause();
        }
        return list;
    }
    public static void fsync(File fileToSync)throws IOException{
        if (!fileToSync.exists()) {
            throw new FileNotFoundException(
                    "File/Directory "+fileToSync.getAbsolutePath()+" does not exist"
            );
        }
        boolean isDir=fileToSync.isDirectory();
        if (isDir && Shell.WINDOWS) {
            return;
        }
        try(FileChannel channel=FileChannel.open(fileToSync.toPath(),
                isDir? StandardOpenOption.READ:StandardOpenOption.WRITE)){
            fsync(channel,isDir);
        }
    }
    public static void fsync(FileChannel channel,boolean isDir)throws IOException{
        try {
            channel.force(true);
        }catch (IOException e){
            if (isDir) {
                assert !(Shell.LINUX
                ||Shell.MAC): "On Linux and MacOSX fsyncing a directory should not throw IOException, " +
                        "we just don't want to rely on that in production (undocumented). Got: "+e;
                return;
            }
            throw e;
        }
    }
    public static IOException wrapException(final String path,
                                            final String methodName,final IOException exception){
        if (exception instanceof InterruptedIOException
        || exception instanceof PathIOException){
            return exception;
        }else {
            String msg=String.format("Failed with %s while processing file/directory :[%s] in" +
                    " method: [%s]",exception.getClass().getName(),path,methodName);
            try {
                return wrapWithMessage(exception,msg);
            }catch (Exception e){
                return new PathIOException(path,exception);
            }
        }
    }
    @SuppressWarnings("unchecked")
    private static <T extends IOException> T wrapWithMessage(
            final T exception,final String msg
    )throws T{
        Class<? extends IOException> clazz = exception.getClass();
        try {
            Constructor<? extends IOException> ctor = clazz.getConstructor(String.class);
            IOException t = ctor.newInstance(msg);
            return (T) (t.initCause(exception));
        }catch (Throwable e){
            throw exception;
        }
    }
    public static byte[] readFullyToByteArray(DataInput in)throws IOException{
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            while (true){
                baos.write(in.readByte());
            }
        }catch (EOFException e){

        }
        return baos.toByteArray();
    }
}
