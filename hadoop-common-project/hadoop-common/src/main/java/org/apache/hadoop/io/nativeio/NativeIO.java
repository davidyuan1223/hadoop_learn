package org.apache.hadoop.io.nativeio;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import com.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.util.CleanerUtil;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.PerformanceAdvisory;
import org.apache.hadoop.util.Shell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileDescriptor;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

        private static boolean workaroundNonThreadSafePasswdCalls=false;
        private static final Map<Integer,CachedName> USER_ID_NAME_CACHE=new ConcurrentHashMap<>();
        private static final Map<Integer,CachedName> GROUP_ID_NAME_CACHE=new ConcurrentHashMap<>();
        public static final int MMAP_PROT_READ=0x1;
        public static final int MMAP_PROT_WRITE=0x2;
        public static final int MMAP_PROT_EXEC=0x04;

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
                    workaroundNonThreadSafePasswdCalls=conf.getBoolean(
                            WORKAROUND_NON_THREADSAFE_CALLS_KEY,WORKAROUND_NON_THREADSAFE_CALLS_DEFAULT
                    );
                    initNative();
                    nativeLoaded=true;
                    cacheTimeout=conf.getLong(
                            CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY,
                            CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_DEFAULT*1000
                    );
                    LOG.debug("Initialized cache for IDs to User/Group mapping with a cache timeout of"+
                            cacheTimeout/1000+" seconds.");
                }catch (Throwable t){
                    PerformanceAdvisory.LOG.debug("Unable to initialize NativeIO libraries",t);
                }
            }
        }

        public static boolean isAvailable(){
            return NativeCodeLoader.isNativeCodeLoaded()&&nativeLoaded;
        }
        private static void assertCodeLoaded()throws IOException{
            if (!isAvailable()) {
                throw new IOException("NativeIO was not loaded");
            }
        }
        public static native FileDescriptor open(String path,int flags,int mode)throws IOException;
        public static native Stat fstat(FileDescriptor fd)throws IOException;
        public static native Stat stat(String path)throws IOException;
        public static native void chmodImpl(String path,int mode)throws IOException;
        public static void chmod(String path,int mode)throws IOException{
            if (!Shell.WINDOWS){
                chmodImpl(path,mode);
            }else {
                try {
                    chmodImpl(path,mode);
                }catch (NativeIOException e){
                    if (e.getErrorCode() == 3) {
                        throw new NativeIOException("No such file or directory",Errno.ENOENT);
                    }else {
                        LOG.warn(String.format("NativeIO.chmod error (%d): %s",e.getErrorCode(),e.getMessage()));
                        throw new NativeIOException("Unknown error",Errno.UNKNOWN);
                    }
                }
            }
        }
        static native void posix_fadvise(FileDescriptor fd,long offset,long len,int flags)throws NativeIOException;
        static native void sync_file_range(FileDescriptor fd,long offset,long nbytes,int flags)throws NativeIOException;
        static void posixFadvisedIfPossible(String identifier,FileDescriptor fd,long offset,long len,int flags)throws NativeIOException{
            if (nativeLoaded && fadvisePossible) {
                try {
                    posix_fadvise(fd,offset,len,flags);
                }catch (UnsatisfiedLinkError ule){
                    fadvisePossible=false;
                }
            }
        }
        public static void syncFileRangeIfPossible(FileDescriptor fd,long offsey,long nbytes,int flags)throws NativeIOException{
            if (nativeLoaded && syncFileRangePossible) {
                try {
                    sync_file_range(fd,offsey,nbytes,flags);
                }catch (UnsupportedOperationException | UnsatisfiedLinkError e){
                    syncFileRangePossible=false;
                }
            }
        }
        static native void mlock_native(ByteBuffer buffer,long len)throws NativeIOException;
        static void mlock(ByteBuffer buffer,long len)throws IOException{
            assertCodeLoaded();
            if (!buffer.isDirect()) {
                throw new IOException("Cannot mlock a non-direct ByteBuffer");
            }
            mlock_native(buffer,len);
        }
        public static void munmap(MappedByteBuffer buffer){
            if (CleanerUtil.UNMAP_SUPPORTED) {
                try {
                    CleanerUtil.getCleaner().freeBuffer(buffer);
                }catch (IOException e){
                    LOG.info("Failed to unmap the buffer",e);
                }
            }else {
                LOG.trace(CleanerUtil.UNMAP_NOT_SUPPORTED_REASON);
            }
        }
        private static native long getUIDforFDOwnerforOwner(FileDescriptor fd)throws IOException;
        private static native String getUserName(long uid)throws IOException;
        private static native String getGroupName(long uid)throws IOException;
        public static Stat getFstat(FileDescriptor fd)throws IOException{
            Stat stat=null;
            if (!Shell.WINDOWS){
                stat=fstat(fd);
                stat.owner=getName(IdCache.USER,stat.ownerId);
                stat.group=getName(IdCache.GROUP,stat.groupId);
            }else {
                try {
                    stat=fstat(fd);
                }catch (NativeIOException e){
                    if (e.getErrorCode() == 6) {
                        throw new NativeIOException("The handle is invalid.",Errno.EBADF);
                    }else {
                        LOG.warn(String.format("NativeIO.getFstat error (%d): %s",e.getErrorCode(),e.getMessage()));
                        throw new NativeIOException("Unknown error",Errno.UNKNOWN);
                    }
                }
            }
            return stat;
        }
        public static Stat getStat(String path)throws IOException{
            if (path == null) {
                String errMessage="Path is null";
                LOG.warn(errMessage);
                throw new IOException(errMessage);
            }
            Stat stat=null;
            try {
                if (!Shell.WINDOWS){
                    stat=stat(path);
                    stat.owner=getName(IdCache.USER,stat.ownerId);
                    stat.group=getName(IdCache.GROUP,stat.groupId);
                }else {
                    stat=stat(path);
                }
            }catch (NativeIOException nioe) {
                LOG.warn("NativeIO.getStat error ({}): {} -- file path: {}",
                        nioe.getErrorCode(), nioe.getMessage(), path);
                throw new PathIOException(path, nioe);
            }
            return stat;
        }

        private static String getName(IdCache domain,int id)throws IOException{
            Map<Integer,CachedName> idNameCache=(domain==IdCache.USER)?USER_ID_NAME_CACHE:GROUP_ID_NAME_CACHE;
            String name;
            CachedName cachedName = idNameCache.get(id);
            long now=System.currentTimeMillis();
            if (cachedName != null && (cachedName.timestamp + cacheTimeout) > now) {
                name=cachedName.name;
            }else {
                name=(domain==IdCache.USER)?getUserName(id):getGroupName(id);
                if (LOG.isDebugEnabled()) {
                    String type=(domain==IdCache.USER)?"UserName":"GroupName";
                    LOG.debug("Got "+type+" "+name+" for ID "+id+" from the native implementation");
                }
                cachedName=new CachedName(name,now);
                idNameCache.put(id,cachedName);
            }
            return name;
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
        public static native long mmap(FileDescriptor fd,int prot,boolean shared,long length)throws IOException;
        public static native void munmap(long addr,long length)throws IOException;


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
        public static class Stat{
            private int ownerId,groupId;
            private String owner,group;
            private int mode;
            public static int S_IFMT=-1;
            public static int S_IFIFO=-1;
            public static int S_IFCHR=-1;
            public static int S_IFDIR=-1;
            public static int S_IFBLK=-1;
            public static int S_IFLNK=-1;
            public static int S_IFSOCK=-1;
            public static int S_ISUID=-1;
            public static int S_ISGID=-1;
            public static int S_ISVTX=-1;
            public static int S_IRUSR=-1;
            public static int S_IWUSR=-1;
            public static int S_IXUSR=-1;
            Stat(int ownerId,int groupId,int mode){
                this.ownerId=ownerId;
                this.groupId=groupId;
                this.mode=mode;
            }
            Stat(String owner,String group,int mode){
                if (!Shell.WINDOWS){
                    this.owner=owner;
                }else {
                    this.owner=stripDomain(owner);
                }
                if (!Shell.WINDOWS){
                    this.group=group;
                }else {
                    this.group=stripDomain(group);
                }
                this.mode=mode;
            }

            @Override
            public String toString() {
                return "Stat(owner='" + owner + "', group='" + group + "'" +
                        ", mode=" + mode + ")";
            }

            public String getOwner() {
                return owner;
            }

            public String getGroup() {
                return group;
            }

            public int getMode() {
                return mode;
            }
        }
        private enum IdCache { USER,GROUP }
        private static class CachedName{
            final long timestamp;
            final String name;
            public CachedName(String name,long timestamp){
                this.name=name;
                this.timestamp=timestamp;
            }
        }
    }

}
