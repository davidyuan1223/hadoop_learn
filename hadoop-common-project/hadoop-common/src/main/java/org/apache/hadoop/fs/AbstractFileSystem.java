package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import com.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.impl.AbstractFSBuilderImpl;
import org.apache.hadoop.fs.impl.OpenFileParameters;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.LambdaUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.zookeeper.Op;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.krb5.Config;
import sun.security.util.AuthResources;

import javax.swing.undo.UndoableEditSupport;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileAlreadyExistsException;
import java.security.AccessControlException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/29
 **/
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class AbstractFileSystem implements PathCapabilities{
    static final Logger LOG= LoggerFactory.getLogger(AbstractFileSystem.class);
    private static final Map<URI,Statistics> STATISTICS_TABLE=new HashMap<>();
    private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE=new HashMap<>();
    private static final Class<?>[] URI_CONFIG_ARGS=new Class[]{URI.class, Configuration.class};
    protected Statistics statistics;

    @VisibleForTesting
    static final String NO_ABSTRACT_FS_ERROR="No AbstractFileSystem configure for scheme";

    private final URI myUri;

    public Statistics getStatistics(){
        return statistics;

    }
    public boolean isValidName(String src){
        StringTokenizer tokenizer = new StringTokenizer(src, Path.SEPARATOR);
        while (tokenizer.hasMoreTokens()) {
            String element = tokenizer.nextToken();
            if (element.equals("..")||
                    element.equals(".")||
                    element.contains(":")){
                return false;
            }
        }
        return true;
    }
    @SuppressWarnings("unchecked")
    static <T> T newInstance(Class<T> theClass,URI uri,Configuration conf){
        T result;
        try {
            Constructor<T> meth=(Constructor<T>)CONSTRUCTOR_CACHE.get(theClass);
            if (meth == null) {
                meth=theClass.getDeclaredConstructor(URI_CONFIG_ARGS);
                meth.setAccessible(true);
                CONSTRUCTOR_CACHE.put(theClass,meth);
            }
            result=meth.newInstance(uri,conf);
        }catch (InvocationTargetException e){
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException)cause;
            }else {
                throw new RuntimeException(cause);
            }
        }catch (Exception e){
            throw new RuntimeException(e);
        }
        return result;
    }
    public static AbstractFileSystem createFileSystem(URI uri, Configuration conf)throws UnsupportedFileSystemException{
        final String fsImplConf=String.format("fs.AbstractFileSystem.%s.impl",uri.getScheme());
        Class<?> clazz = conf.getClass(fsImplConf, null);
        if (clazz == null) {
            throw new UnsupportedFileSystemException(String.format(
                    "%s=null: %s: %s",fsImplConf,NO_ABSTRACT_FS_ERROR,uri.getScheme()
            ));
        }
        return (AbstractFileSystem)newInstance(clazz,uri,conf);
    }
    protected static synchronized Statistics getStatistics(URI uri){
        String scheme = uri.getScheme();
        if (scheme == null) {
            throw new IllegalArgumentException("Scheme not defined in the uri: "+uri);
        }
        URI baseUri=getBaseUri(uri);
        Statistics result = STATISTICS_TABLE.get(baseUri);
        if (result == null) {
            result=new Statistics(scheme);
            STATISTICS_TABLE.put(baseUri,result);
        }
        return result;
    }
    private static URI getBaseUri(URI uri){
        String scheme = uri.getScheme();
        String authority = uri.getAuthority();
        String baseUriString=scheme+"://";
        if (authority != null) {
            baseUriString=baseUriString+authority;
        }else {
            baseUriString+="/";
        }
        return URI.create(baseUriString);
    }
    public static synchronized void clearStatistics(){
        for (Statistics stat : STATISTICS_TABLE.values()) {
            stat.reset();
        }
    }
    public static synchronized void printStatistics(){
        for (Map.Entry<URI, Statistics> entry : STATISTICS_TABLE.entrySet()) {
            System.out.println(" FileSystem "+entry.getKey().getScheme()+"://"+
                    entry.getKey().getAuthority()+":"+entry.getValue());
        }
    }
    protected static synchronized Map<URI,Statistics> getAllStatistics(){
        Map<URI,Statistics> statsMap=new HashMap<>(STATISTICS_TABLE.size());
        for (Map.Entry<URI, Statistics> entry : STATISTICS_TABLE.entrySet()) {
            URI key = entry.getKey();
            Statistics value = entry.getValue();
            Statistics newStatsObj=new Statistics(value);
            statsMap.put(URI.create(key.toString()),newStatsObj);
        }
        return statsMap;
    }
    public static AbstractFileSystem get(final URI uri,final Configuration conf)throws UnsupportedFileSystemException{
        return createFileSystem(uri,conf);
    }
    public AbstractFileSystem(final URI uri,String supportedScheme,
                              final boolean authorityNeeded,
                              final int defaultPort)throws URISyntaxException{
        myUri=getUri(uri,supportedScheme,authorityNeeded,defaultPort);
        statistics=getStatistics(uri);
    }
    public void checkScheme(URI uri,String supportedScheme){
        String scheme = uri.getScheme();
        if (scheme == null) {
            throw new HadoopIllegalArgumentException("Uri without scheme: "+uri);
        }
        if (!scheme.equals(supportedScheme)) {
            throw new HadoopIllegalArgumentException("Uri scheme "+uri
            +" does not match the scheme "+supportedScheme);
        }
    }
    private URI getUri(URI uri,String supportedScheme,boolean authorityNeeded,int defaultPort)throws URISyntaxException{
        checkScheme(uri,supportedScheme);
        if (defaultPort < 0 && authorityNeeded) {
            throw new HadoopIllegalArgumentException("FileSystem implementation error - default port "+defaultPort
            +" is not valid");
        }
        String authority = uri.getAuthority();
        if (authority == null) {
            if (authorityNeeded) {
                throw new HadoopIllegalArgumentException("Uri without authority: "+uri);
            }else {
                return new URI(supportedScheme+"//");
            }
        }
        int port = uri.getPort();
        port=(port==-1?defaultPort:port);
        if (port==-1) {
            return new URI(supportedScheme,authority,"/",null);
        }
        return new URI(supportedScheme+"://"+uri.getHost()+":"+port);
    }
    public abstract int getUriDefaultPort();
    public URI getUri(){return myUri;}
    public void checkPath(Path path){
        URI uri = path.toUri();
        String scheme = uri.getScheme();
        String authority = uri.getAuthority();
        if (scheme == null) {
            if (authority == null) {
                if (path.isUriPathAbsolute()) {
                    return;
                }
                throw new InvalidPathException("relative paths not allowed: "+path);
            }
        }
        String scheme1 = this.getUri().getScheme();
        String host1 = this.getUri().getHost();
        String host = uri.getHost();
        if (!scheme1.equalsIgnoreCase(scheme) ||
                (host1 != null && !host1.equalsIgnoreCase(host))
                || (host1 == null && host != null)) {
            throw new InvalidPathException("Wrong FS: "+path+", expected: "+this.getUri());
        }
        int port1 = this.getUri().getPort();
        int port = uri.getPort();
        if (port1==-1) {
            port1=this.getUriDefaultPort();
        }
        if (port != port1) {
            throw new InvalidPathException("Wrong FS: "+path
            +" and ports="+port
            +", expected: "+this.getUri()+" woth port="+port1);
        }
    }
    public String getUriPath(final Path p){
        checkPath(p);
        String path = p.toUri().getPath();
        if (!isValidName(path)) {
            throw new InvalidPathException("Path part "+path+"from URI "+
                    p+" is not a valid filename");
        }
        return path;
    }
    public Path markQualified(Path path){
        checkPath(path);
        return path.makeQualified(this.getUri(),null);
    }
    public Path getInitialWorkingDirectory(){
        return null;
    }
    public Path getHomeDirectory(){
        String username;
        try {
            username= UserGroupInformation.getCurrentUser().getShortUserName();
        }catch (IOException e){
            LOG.warn("Unable to get user name. Fall back to system property user.name",e);
            username=System.getProperty("user.name");
        }
        return new Path("/user/"+username)
                .makeQualified(getUri(),null);
    }
    @Deprecated
    public abstract FsServerDefaults getServerDefaults()throws IOException;

    public FsServerDefaults getServerDefaults(final Path f)throws IOException{
        return getServerDefaults();
    }
    public Path resolvePath(final Path p)throws FileNotFoundException,
            UnresolvedLinkException, AccessControlException,IOException{
        checkPath(p);
        return getFileStatus(p).getPath();
    }
    public final FSDataOutputStream create(final Path f,
                                           final EnumSet<CreateFlag> createFlag,
                                           Options.CreateOpts... opts)
        throws AccessControlException, FileAlreadyExistsException,FileNotFoundException,
            ParentNotDirectoryException,UnsupportedFileSystemException,UnresolvedLinkException,
            IOException{
        checkPath(f);
        int bufferSize=-1;
        short replication=-1;
        long blockSize=-1;
        int bytesPerChecksum=-1;
        Options.ChecksumOpt checksumOpt=null;
        FsPermission permission=null;
        Progressable progress=null;
        Boolean createParent=null;
        for (Options.CreateOpts opt : opts) {
            if (Options.CreateOpts.BlockSize.class.isInstance(opt)) {
                if (blockSize != -1) {
                    throw new HadoopIllegalArgumentException("BlockSize option is set multiple times");
                }
                blockSize=((Options.CreateOpts.BlockSize)opt).getValue();
            } else if (Options.CreateOpts.BufferSize.class.isInstance(opt)) {
                if (bufferSize != -1) {
                    throw new HadoopIllegalArgumentException("BufferSize option is set multiple times");
                }
                bufferSize=((Options.CreateOpts.BufferSize)opt).getValue();
            } else if (Options.CreateOpts.ReplicationFactor.class.isInstance(opt)) {
                if (replication != -1) {
                    throw new HadoopIllegalArgumentException("" +
                            "ReplicationFactor option is set multiple times");
                }
                replication=((Options.CreateOpts.ReplicationFactor)opt).getValue();
            } else if (Options.CreateOpts.BytesPerChecksum.class.isInstance(opt)) {
                if (bytesPerChecksum != -1) {
                    throw new HadoopIllegalArgumentException("" +
                            "BytesPerChecksum option is set multiple times");
                }
                bytesPerChecksum=((Options.CreateOpts.BytesPerChecksum)opt).getValue();
            }else if (Options.CreateOpts.ChecksumParam.class.isInstance(opt)) {
                if (checksumOpt != null) {
                    throw new HadoopIllegalArgumentException("" +
                            "ChecksumParam option is set multiple times");
                }
                checksumOpt=((Options.CreateOpts.ChecksumParam)opt).getValue();
            }else if (Options.CreateOpts.Perms.class.isInstance(opt)) {
                if (permission != null) {
                    throw new HadoopIllegalArgumentException("" +
                            "Perms option is set multiple times");
                }
                permission=((Options.CreateOpts.Perms)opt).getValue();
            }else if (Options.CreateOpts.Progress.class.isInstance(opt)) {
                if (progress != null) {
                    throw new HadoopIllegalArgumentException("" +
                            "BytesPerChecksum option is set multiple times");
                }
                progress=((Options.CreateOpts.Progress)opt).getValue();
            }else if (Options.CreateOpts.CreatParent.class.isInstance(opt)) {
                if (createParent != null) {
                    throw new HadoopIllegalArgumentException("" +
                            "CreatParent option is set multiple times");
                }
                createParent=((Options.CreateOpts.CreatParent)opt).getValue();
            }else {
                throw new HadoopIllegalArgumentException("Unknown CreateOpts of type "+opt.getClass().getName());
            }
        }
        if (permission == null) {
            throw new HadoopIllegalArgumentException("no permission supplied");
        }
        FsServerDefaults ssDef=getServerDefaults(f);
        if (ssDef.getBlockSize() % ssDef.getBytesPerChecksum() != 0) {
            throw new IOException("Internal error: default blockSize is not a multiple of default byesPerChecksum ");
        }
        if (blockSize==-1) {
            blockSize=ssDef.getBlockSize();
        }
        Options.ChecksumOpt defaultOpt=new Options.ChecksumOpt(
                ssDef.getChecksumType(),
                ssDef.getBytesPerChecksum()
        );
        checksumOpt= Options.ChecksumOpt.processChecksumOpt(defaultOpt,checksumOpt,bytesPerChecksum);
        if (bufferSize==-1) {
            bufferSize=ssDef.getFileBufferSize();
        }
        if (replication==-1) {
            replication=ssDef.getReplication();
        }
        if (createParent == null) {
            createParent=false;
        }
        if (blockSize % bytesPerChecksum != 0) {
            throw new HadoopIllegalArgumentException("blockSize should be a multiple of checksumsize");
        }
        return this.createInterval(f,createFlag,permission,bufferSize,replication,
                blockSize,progress,checksumOpt,createParent);
    }

    protected abstract FSDataOutputStream createInterval(Path f, EnumSet<CreateFlag> flag, FsPermission absolutePermission, int bufferSize, short replication, long blockSize, Progressable progress, Options.ChecksumOpt checksumOpt, Boolean createParent);;
    public abstract void mkdir(final Path dir,final FsPermission permission,
                               final boolean createParent);
    public abstract boolean delete(final Path f,final boolean recursive);
    public FSDataInputStream open(final Path f){
        return open(f,getServerDefaults(f).getFileBufferSize());
    }
    public abstract FSDataInputStream open(final Path f,int bufferSize);
    public boolean truncate(Path f,long newLength){
        throw new UnsupportedOperationException(getClass().getSimpleName()+
                "doesn't support truncate");
    }
    public abstract boolean setReplication(final Path f,final short replication);
    public final void rename(final Path src,final Path dst,final Options.Rename... options) throws IOException {
        boolean overwrite=false;
        if (options != null) {
            for (Options.Rename option : options) {
                if (option== Options.Rename.OVERWRITE){
                    overwrite=true;
                }
            }
        }
        renameInternal(src,dst,overwrite);
    }
    public abstract void renameInternal(final Path src,final Path dst);
    public void renameInternal(final Path src,final Path dst,boolean overwrite) throws IOException {
        final FileStatus srcStatus=getFileStatus(src);
        FileStatus dstStatus;
        try{
            dstStatus=getFileLinkStatus(dst);
        }catch (IOException e){
            dstStatus=null;
        }
        if (dstStatus != null) {
            if (dst.equals(src)) {
                throw new FileAlreadyExistsException("The source "+src+" and destincation "+dst+" are the same");
            }
            if (srcStatus.isSymlink() && dst.equals(srcStatus.getSymlink())){
                throw new FileAlreadyExistsException("Cannot rename symlink "+src+" to its  target "+dst);
            }
            if (srcStatus.isDirectory() != dstStatus.isDirectory()) {
                throw new IOException("Source "+src+" and destination "+dst+" must both be directories");
            }
            if (!overwrite) {
                throw new FileAlreadyExistsException("Rename destination "+dst+" alreaday exists.");
            }
            if (dstStatus.isDirectory()) {
                RemoteIterator<FileStatus> list=listStatusIterator(dst);
                if (list != null && list.hasNext()) {
                    throw new IOException(
                            "Rename cannot overwrite non empty destination directory"+dst
                    );
                }
            }
            delete(dst,false);
        }else {
            final Path parent=dst.getParent();
            final FileStatus parentStatus=getFileStatus(parent);
            if (parentStatus.isFile()) {
                throw new ParentNotDirectoryException("Rename destination parent "+parent+" is a file.");
            }
        }
        renameInternal(src,dst);
    }
    public boolean supportSymlinks(){return false;}
    public void createSymlink(final Path target,final Path link,
                              final boolean createParent) throws IOException {
        throw new IOException("File System does not support symlinks");
    }
    public Path getLinkTarget(final Path f){
        throw new AssertionError("Implementation Error: "+getClass()
        +" that threw an UnresolvedLinkException,causing this method to be called," +
                "needs to override this method.");
    }
    public abstract void setPermission(final Path f,final FsPermission permission);
    public abstract void setOwner(final Path f,final String username,final String groupname);
    public abstract void setTimes(final Path f,final long mtime,final Long atime);
    public abstract FileChecksum getFileChecksum(final Path f);
    public abstract FileStatus getFileStatus(final Path f);
    public void msync(){
        throw new UnsupportedOperationException(getClass().getCanonicalName()+
                " does not support method msync");
    }
    @InterfaceAudience.LimitedPrivate({"HDFS","Hive"})
    public void access(Path path,FsAction mode){
        FileSystem.checkAccessPermissions(this.getFileStatus(path),mode);
    }
    public FileStatus getFileLinkStatus(final Path f){
        return getFileStatus(f);
    }
    public abstract BlockLocation[] getFileBlockLocations(final Path f,
                                                          final long start,
                                                          final long len);
    public FsStatus getFsStatus(final Path f){
        return getFsStatus();
    }
    public abstract FsStatus getFsStatus();
    public RemoteIterator<FileStatus> listStatusIterator(final Path f){
        return new RemoteIterator<FileStatus>() {
            private int i=0;
            private FileStatus[] statusList=listStatus(f);
            @Override
            public boolean hasNext() throws IOException {
                return i<statusList.length;
            }

            @Override
            public FileStatus next() throws IOException {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return statusList[i++];
            }
        };
    }
    public RemoteIterator<LocatedFileStatus> listLocatedFileStatus(final Path f){
        return new RemoteIterator<LocatedFileStatus>() {
            private RemoteIterator<FileStatus> itor=listStatusIterator(f);
            @Override
            public boolean hasNext() throws IOException {
                return itor.hasNext();
            }

            @Override
            public LocatedFileStatus next() throws IOException {
                if (!hasNext()) {
                    throw new NoSuchElementException("No more entry in "+f);
                }
                FileStatus result = itor.next();
                BlockLocation[] locs=null;
                if (result.isFile()) {
                    locs=getFileBlockLocations(result.getPath(),0,result.getLen());
                }
                return new LocatedFileStatus(result,locs);
            }
        };
    }
    public abstract FileStatus[] listStatus(final Path f);
    public RemoteIterator<Path> listCorruptFileBlocks(Path path){
        throw new UnsupportedOperationException(getClass().getCanonicalName()
        +" does not support listCorruptFileBlocks");
    }
    public abstract void setVerifyChecksum(final boolean verifyChecksum);
    public String getCanonicalServiceName(){
        return SecurityUtil.buildDTServiceName(getUri(),getUriDefaultPort());
    }
    @InterfaceAudience.LimitedPrivate({"HDFS","MapReduce"})
    public List<Token<?>> getDelegationTokens(String renewer){
        return Collections.emptyList();
    }
    public void modifyAclEntries(Path path, List<AclEntry> aclSpec){
        throw new UnsupportedOperationException(getClass().getCanonicalName()
        +" doesn't support modifyAclEntries");
    }
    public void removeAclEntries(Path path,List<AclEntry> aclSpec){
        throw new UnsupportedOperationException(getClass().getCanonicalName()
                +" doesn't support removeAclEntries");
    }
    public void removeDefaultAcl(Path path){
        throw new UnsupportedOperationException(getClass().getCanonicalName()
                +" doesn't support removeDefaultAcl");
    }
    public void removeAcl(Path path){
        throw new UnsupportedOperationException(getClass().getCanonicalName()
                +" doesn't support removeAcl");
    }
    public void setAcl(Path path,List<AclEntry> aclSpec){
        throw new UnsupportedOperationException(getClass().getCanonicalName()
        +" doesn't support setAcl");
    }
    public AclStatus getAclStatus(Path path){
        throw new UnsupportedOperationException(getClass().getCanonicalName()
        +" doesn't support getAclStatus");
    }
    public void setXAttr(Path path,String name,byte[] value){
        setXAttr(path,name,value,EnumSet.of(XAttrSetFlag.CREATE,XAttrSetFlag.REPLACE));
    }
    public void setXAttr(Path path,String name,byte[] value,EnumSet<XAttrSetFlag> flag){
        throw new UnsupportedOperationException(getClass().getCanonicalName()
        +" doesn't support setXAttr");
    }
    public byte[] getXAttr(Path path,String name){
        throw new UnsupportedOperationException(getClass().getCanonicalName()
        +" doesn't support getXAttr");
    }
    public Map<String ,byte[]> getXAttrs(Path path){
        throw new UnsupportedOperationException(getClass().getCanonicalName()
        +" doesn't support getXAttrs");
    }
    public Map<String ,byte[]> getXAttrs(Path path,List<String > names){
        throw new UnsupportedOperationException(getClass().getCanonicalName()
                +" doesn't support getXAttrs");
    }
    public List<String > listXAttrs(Path path){
        throw new UnsupportedOperationException(getClass().getCanonicalName()
        +" doesn't support listXAttrs");
    }
    public void removeXAttr(Path path,String name){
        throw new UnsupportedOperationException(getClass().getCanonicalName()
        +" doesn't support removeXAttr");
    }
    public Path createSnapshot(final Path path,final String snapshotName){
        throw new UnsupportedOperationException(getClass().getCanonicalName()
        +" doesn't support createSnapshot");
    }
    public Path renameSnapshot(final Path path,final String snapshotOldName,final String snapshotNewName){
        throw new UnsupportedOperationException(getClass().getCanonicalName()
                +" doesn't support renameSnapshot");
    }
    public Path deleteSnapshot(final Path snapshotDir,final String snapshotName){
        throw new UnsupportedOperationException(getClass().getCanonicalName()
                +" doesn't support deleteSnapshot");
    }
    public void satisfyStoragePolicy(final Path path){
        throw new UnsupportedOperationException(getClass().getCanonicalName()
        +" doesn't support satisfyStoragePolicy");
    }
    public void setStoragePolicy(final Path path,final String policyName){
        throw new UnsupportedOperationException(getClass().getCanonicalName()
                +" doesn't support setStoragePolicy");
    }
    public void unsetStoragePolicy(final Path src){
        throw new UnsupportedOperationException(getClass().getCanonicalName()
                +" doesn't support unsetSoragePolicy");
    }
    public BlockStoragePolicySpi getStoragePolicy(final Path path){
        throw new UnsupportedOperationException(getClass().getCanonicalName()
        +" doesn't support getStoragePolicy");
    }
    public Collection<? extends BlockStoragePolicySpi> getAllStoragePolicies(){
        throw new UnsupportedOperationException(getClass().getCanonicalName()
                +" doesn't support getAllStoragePolicies");
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AbstractFileSystem)) {
            return false;
        }
        return myUri.equals(((AbstractFileSystem) o).myUri);
    }

    @Override
    public int hashCode() {
       return myUri.hashCode();
    }

    public CompletableFuture<FSDataInputStream> openFileWithOptions(Path path,
                                                                    final OpenFileParameters parameters){
        AbstractFSBuilderImpl.rejectUnknownMandatoryKeys(
                parameters.getMandatoryKeys(),
                Collections.emptySet(),
                "for "+path
        );
        return LambdaUtils.eval(new CompletableFuture<>(),()->{
            open(path,parameters.getBufferSize());
        });
    }
    public boolean hasPathCapability(final Path path,final String capability){
        switch (validatePathCapabilityArgs(markQualified(path),capability)){
            case CommonPathCapabilities.FS_SYMLINKS:
                return supportSymlinks();
            default:
                return false;
        }
    }
    @InterfaceStability.Unstable
    public MultipartUploaderBuilder createMultipartUploader(Path basePath){
        methodNotSupported();
        return null;
    }
    protected final void methodNotSupported(){
        String name=Thread.currentThread().getStackTrace()[2].getMethodName();
        throw new UnsupportedOperationException(getClass().getCanonicalName()
        +" does not support method "+name);
    }
}
