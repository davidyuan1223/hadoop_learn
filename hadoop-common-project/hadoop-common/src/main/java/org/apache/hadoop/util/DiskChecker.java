package org.apache.hadoop.util;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/23
 **/
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DiskChecker {
    private static final Logger logger= LoggerFactory.getLogger(DiskChecker.class);
    private static AtomicReference<FileIoProvider> fileIoProvider=
            new AtomicReference<>(new DefaultFileIoProvider());
    public static void checkDir(File dir) throws DiskErrorException {
        checkDirInternal(dir);
    }
    public static void checkDirWithDiskInfo(File dir) throws DiskErrorException {
        checkDirInternal(dir);
        checkDiskInfo(dir);
    }
    private static void checkDirInternal(File dir) throws DiskErrorException {
        if (!mkdirsWithExistsCheck(dir)) {
            throw new DiskErrorException("Cannot create directory: "+dir.toString());
        }
        checkAccessByFileMethods(dir);
    }
    public static void checkDir(LocalFileSystem localFS, Path dir,
                                FsPermission expected)
    public static class DiskErrorException extends IOException{
        public DiskErrorException(String msg){
            super(msg);
        }
        public DiskErrorException(String msg,Throwable cause){
            super(msg,cause);
        }
    }
    public static class DiskOutOfSpaceException extends IOException{
        public DiskOutOfSpaceException(String msg){
            super(msg);
        }
    }
    interface FileIoProvider{
        FileOutputStream get(File f)throws FileNotFoundException;
        void write(FileOutputStream fos,byte[] data) throws IOException;
    }
    private static class DefaultFileIoProvider implements FileIoProvider{

        @Override
        public FileOutputStream get(File f) throws FileNotFoundException {
            return new FileOutputStream(f);
        }

        @Override
        public void write(FileOutputStream fos, byte[] data) throws IOException {
            fos.write(data);
        }
    }
}
