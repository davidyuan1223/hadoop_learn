package org.apache.hadoop.io.nativeio;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import com.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.NativeCodeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileDescriptor;
import java.io.IOException;
import java.nio.ByteBuffer;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class NativeIO {
    public static class POSIX{
        public static int O_RDONLY=-1;
        public static int O_WRONLY=-1;
        public static int O_RDWR=-1;
        public static int O_CREAT=-1;
        public static int O_EXCL=-1;
        public static int O_NOCTTY=-1;
        public static int O_TRUNC=-1;
        public static int O_APPEND=-1;
        public static int O_NONBLOCK=-1;
        public static int O_SYNC=-1;

        public static int POSIX_FADV_NORMAL=-1;
        public static int POSIX_FADV_RANDOM=-1;
        public static int POSIX_FADV_SEQUENTIAL=-1;
        public static int POSIX_FADV_WILLNEED=-1;
        public static int POSIX_FADV_DONTNEED=-1;
        public static int POSIX_FADV_NOREUSE=-1;

        public static int SYNC_FILE_RANGE_WAIT_BEFORE=1;
        public static int SYNC_FILE_RANGE_WRITE=2;
        public static int SYNC_FILE_RANGE_WAIT_AFTER=4;

        private static SupportState pmdkSupportState=SupportState.UNSUPPORTED;
        private static final Logger LOG=LoggerFactory.getLogger(NativeIO.class);
        public static boolean fadvisePossible=false;
        private static boolean nativeLoaded=false;
        private static boolean syncFileRangePossible=true;
        static final String WORKAROUND_NON_THREADSAFE_CALLS_KEY="hadoop.workaround.non.threadsafe.getpwuid";
        static final boolean WORKAROUND_NON_THREADSAFE_CALLS_DEFAULT=true;
        private static long cacheTimeout=-1;
        private static CacheManipulator cacheManipulator=new CacheManipulator();
        public static CacheManipulator getCacheManipulator(){return cacheManipulator;}
        public static void setCacheManipulator(CacheManipulator cacheManipulator){
            POSIX.cacheManipulator=cacheManipulator;
        }
        private static native String getPmdkLibPath();
        private static native boolean isPmemCheck(long address,long length);
        private static native PmemMappedRegion pmemMapFile(String path,long length,boolean isFileExist);
        private static native boolean pmemUnMap(long address,long length);
        private static native void pmemCopy(byte[] src,long dest,boolean isPmem,long length);
        private static native void pmemDrain();
        private static native void pmemSync(long address,long length);

        static {
            if (NativeCodeLoader.isNativeCodeLoaded()) {
                try {
                    Configuration conf = new Configuration();

                }
            }
        }

        public static void setPmdkSupportState(int stateCode) {
            for (SupportState state : SupportState.values()) {
                if (state.getStateCode() == stateCode) {
                    pmdkSupportState=state;
                    return;
                }
            }
            LOG.error("The state code: "+stateCode+" is unrecognized!");
        }
        public static String getPmdkSupportStateMessage(){
            if (getPmdkLibPath() != null) {
                return pmdkSupportState.getMessage()+" The pmdk lib path "+getPmdkLibPath();
            }
            return pmdkSupportState.getMessage();
        }
        public static boolean isPmdkAvailable(){
            LOG.info(pmdkSupportState.getMessage());
            return pmdkSupportState==SupportState.SUPPORTED;
        }


        public enum SupportState{
            UNSUPPORTED(-1),
            PMDK_LIB_NOT_FOUND(1),
            SUPPORTED(0);

            private byte stateCode;
            SupportState(int stateCode){
                this.stateCode=(byte) stateCode;
            }

            public int getStateCode() {
                return stateCode;
            }

            public String getMessage(){
                String msg;
                switch (stateCode){
                    case -1:
                        msg="The native code was build without PMDK support.";
                        break;
                    case 1:
                        msg="The native code was built with PMDK support, but PMDK libs" +
                                " were NOT found in execution environment or failed to be loaded.";
                        break;
                    case 0:
                        msg="The native code was built with PMDK support, and PMDK libs " +
                                "were loaded successfully.";
                        break;
                    default:
                        msg="The state code: "+stateCode+" is unrecognized" ;
                }
                return msg;
            }
        }
        @VisibleForTesting
        public static class CacheManipulator{
            public void mlock(String identifier, ByteBuffer buffer,long len)throws IOException{
                POSIX.mlock(buffer,len);
            }
            public long getMemlockLimit(){
                return NativeIO.getmemlockLimit();
            }
            public long getOperatingSystemPageSize(){
                return NativeIO.getOperationSystemPageSize();
            }
            public void posixFadviseIfPossible(String idetifier, FileDescriptor fd,long offset,long len,int flags)throws NativeIOException{
                NativeIO.POSIX.posixFadvisedIfPossible(idetifier)
            }
        }
        @VisibleForTesting
        public static class NoMlockCacheManipulator extends CacheManipulator{
            public void mlock(String identifier,ByteBuffer buffer,long len)throws IOException{
                LOG.info("mlocking "+identifier);
            }
            public long getMemlockLimit(){
                return 1125899906842624L;
            }
            public long getOperatingSystemPageSize() {
                return 4096;
            }

            public boolean verifyCanMlock() {
                return true;
            }
        }
        public static class PmemMappedRegion{
            private long address;
            private long length;
            private boolean isPmem;
            public PmemMappedRegion(long address,long length,boolean isPmem){
                this.address=address;
                this.length=length;
                this.isPmem=isPmem;
            }
            public boolean isPmem(){return this.isPmem;}

            public long getAddress() {
                return address;
            }

            public long getLength() {
                return length;
            }
        }
        public static class Pmem{
            public static boolean isPmem(long address,long length){
                return NativeIO.POSIX.isPmemCheck(address,length);
            }
            public static PmemMappedRegion mapBlock(String path,long length,boolean isFileExist){
                return NativeIO.POSIX.pmemMapFile(path,length,isFileExist);
            }
            public static boolean unmapBlock(long address,long length){
                return NativeIO.POSIX.pmemUnMap(address,length);
            }
            public static void memCopy(byte[] src,long dest,boolean isPmem,long length){
                NativeIO.POSIX.pmemCopy(src,dest,isPmem,length);
            }
            public static void memSync(PmemMappedRegion region){
                if (region.isPmem()) {
                    NativeIO.POSIX.pmemDrain();
                }else {
                    NativeIO.POSIX.pmemSync(region.getAddress(),region.getLength());
                }
            }
            public static String getPmdkLibPath(){
                return POSIX.getPmdkLibPath();
            }
        }
    }
}
