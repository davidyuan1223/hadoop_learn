package org.apache.hadoop.fs.ftp;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import com.apache.hadoop.classification.VisibleForTesting;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ConnectException;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class FTPFileSystem extends FileSystem {
    private static final Logger LOG= LoggerFactory.getLogger(FTPFileSystem.class);
    public static final int DEFAULT_BUFFER_SIZE=1024*1024;
    public static final int DEFAULT_BLOCK_SIZE=4*1024;
    public static final long DEFAULT_TIMEOUT=0;
    public static final String FS_FTP_USER_PREFIX="fs.ftp.user";
    public static final String FS_FTP_HOST="fs.ftp.host";
    public static final String FS_FTP_HOST_PORT="fs.ftp.host.port";
    public static final String FS_FTP_PASSWORD_PREFIX="fs.ftp.password";
    public static final String FS_FTP_DATA_CONNECTION_MODE="fs.ftp.data.connection.mode";
    public static final String FS_FTP_TRANSFER_MODE="fs.ftp.transfer.mode";
    public static final String E_SAME_DIRECTORY_ONLY="only same directory renames are supported";
    public static final String FS_FTP_TIMEOUT="fs.ftp.timeout";
    private URI uri;

    @Override
    public String getScheme(){
        return "ftp";
    }
    @Override
    protected int getDefaultPort(){
        return FTP.DEFAULT_PORT;
    }

    @Override
    public void initialize(URI uri, Configuration conf)throws IOException{
        super.initialize(uri,conf);
        String host = uri.getHost();
        host=host==null?conf.get(FS_FTP_HOST,null):host;
        if (host == null) {
            throw new IOException("Invalid host specified");
        }
        conf.set(FS_FTP_HOST,host);
        int port = uri.getPort();
        if (port==-1) {
            port=conf.getInt(FS_FTP_HOST_PORT,FTP.DEFAULT_PORT);
        }
        conf.setInt(FS_FTP_HOST_PORT,port);
        String userAndPassword = uri.getUserInfo();
        if (userAndPassword == null) {
            userAndPassword=(conf.get(FS_FTP_USER_PREFIX+host,null)+":"+ conf.get(FS_FTP_PASSWORD_PREFIX+host,null));
        }
        String[] userPasswordInfo = userAndPassword.split(":");
        Preconditions.checkState(userPasswordInfo.length>1,"Invalid username / password");
        conf.set(FS_FTP_USER_PREFIX+host,userPasswordInfo[0]);
        conf.set(FS_FTP_PASSWORD_PREFIX+host,userPasswordInfo[1]);
        setConf(conf);
        this.uri=uri;
    }
    private FTPClient connect()throws IOException{
        FTPClient client=null;
        Configuration conf = getConf();
        String host = conf.get(FS_FTP_HOST);
        int port = conf.getInt(FS_FTP_HOST_PORT, FTP.DEFAULT_PORT);
        String user = conf.get(FS_FTP_USER_PREFIX + host);
        String password = conf.get(FS_FTP_PASSWORD_PREFIX + host);
        client=new FTPClient();
        client.connect(host,port);
        int reply=client.getReplyCode();
        if (!FTPReply.isPositiveCompletion(reply)) {
            throw NetUtils.wrapException(host,port,new ConnectException("Server response "+reply));
        } else if (client.login(user, password)) {
            client.setFileTransferMode(getTransferMode(conf));
            client.setFileType(FTP.BINARY_FILE_TYPE);
            client.setBufferSize(DEFAULT_BUFFER_SIZE);
            setTimeout(client,conf);
            setDataConnectionMode(client,conf);
        }else {
            throw new IOException("Login failed on server - "+host+", port - "+port+" as user "+user);
        }
        return client;
    }
    @VisibleForTesting
    void setTimeout(FTPClient client,Configuration conf){
        long timeout = conf.getLong(FS_FTP_TIMEOUT, DEFAULT_TIMEOUT);
        client.setControlKeepAliveTimeout(timeout);
    }
    @VisibleForTesting
    int getTransferMode(Configuration conf){
        final String mode = conf.get(FS_FTP_TRANSFER_MODE);
        int ret=FTP.BLOCK_TRANSFER_MODE;
        if (mode == null) {
            return ret;
        }
        final String upper = mode.toUpperCase();
        if (upper.equals("STREAM_TRANSFER_MODE")) {
            ret=FTP.STREAM_TRANSFER_MODE;
        } else if (upper.equals("COMPRESSED_TRANSFER_MODE")) {
            ret=FTP.COMPRESSED_TRANSFER_MODE;
        }else {
            if (!upper.equals("BLOCK_TRANSFER_MODE")) {
                LOG.warn("Cannot parse the value for "+FS_FTP_TRANSFER_MODE+": "+mode+". Using default");
            }
        }
        return ret;
    }
    @VisibleForTesting
    void setDataConnectionMode(FTPClient client,Configuration conf)throws IOException{
        final String mode = conf.get(FS_FTP_DATA_CONNECTION_MODE);
        if (mode == null) {
            return;
        }
        final String upper=mode.toUpperCase();
        if (upper.equals("PASSIVE_LOCAL_DATA_CONNECTION_MODE")) {
            client.enterLocalPassiveMode();
        } else if (upper.equals("PASSIVE_REMOTE_DATA_CONNECTION_MODE")) {
            client.enterRemotePassiveMode();
        }else {
            if (!upper.equals("ACTIVE_LOCAL_DATA_CONNECTION_MODE")) {
                LOG.warn("Cannot parse the value for "+FS_FTP_DATA_CONNECTION_MODE+": "+mode+". Using default");
            }
        }
    }
    private void disconnect(FTPClient client)throws IOException{
        if (client != null) {
            if (!client.isConnected()) {
                throw new FTPException("Client not connected");
            }
            boolean logoutSuccess=client.logout();
            client.disconnect();
            if (!logoutSuccess) {
                LOG.warn("Logout failed while disconnecting, error code - "+client.getReplyCode());
            }
        }
    }
    private Path makeAbsolute(Path workDir,Path path){
        if (path.isAbsolute()) {
            return path;
        }
        return new Path(workDir,path);
    }
    @Override
    public FSDataInputStream open(Path file,int bufferSize)throws IOException{
        FTPClient client = connect();
        Path workDir = new Path(client.printWorkingDirectory());
        Path absolute = makeAbsolute(workDir, file);
        FileStatus fileStat=getFileStatus(client,absolute);
        if (fileStat.isDirectory()) {
            disconnect(client);
            throw new FileNotFoundException("Path "+file+" is a directory.");
        }
        client.allocate(bufferSize);
        Path parent = absolute.getParent();
        client.changeWorkingDirectory(parent.toUri().getPath());
        InputStream is = client.retrieveFileStream(file.getName());
        FSDataInputStream fis = new FSDataInputStream(new FTPInputStream(is, client, statistics));
        if (!FTPReply.isPositivePreliminary(client.getReplyCode())) {
            fis.close();
            throw new IOException("Unable to open file: "+file+", Aborting");
        }
        return fis;
    }
    @Override
    public FSDataOutputStream create(Path file, FsPermission permission, boolean overrwrite, int bufferSize, short replication, long blockSize, Progressable progressable)throws IOException{
        final FTPClient client=connect();
        Path workDir = new Path(client.printWorkingDirectory());
        Path absolute = makeAbsolute(workDir, file);
        FileStatus fileStatus;
        try {
            fileStatus=getFileStatus(client,file);
        }catch (FileNotFoundException e){
            fileStatus=null;
        }
        if (fileStatus != null) {
            if (overrwrite && !fileStatus.isDirectory()) {
                delete(client,file,false);
            }else {
                disconnect(client);
                throw new FileAlreadyExistsException("File already exists: "+file);
            }
        }
        Path parent = absolute.getParent();
        if (parent==null || !mkdirs(client,parent,FsPermission.getDirDefault())){
            parent=(parent==null)?new Path("/"):parent;
            disconnect(client);
            throw new IOException("create(): Mkdirs failed to created: "+parent);
        }
        client.allocate(bufferSize);
        client.changeWorkingDirectory(parent.toUri().getPath());
        OutputStream outputStream = client.storeFileStream(file.getName());
        if (!FTPReply.isPositivePreliminary(client.getReplyCode())) {
            if (outputStream != null) {
                IOUtils.closeStream(outputStream);
            }
            disconnect(client);
            throw new IOException("Unable to create file: "+file+",Aborting");
        }
        FSDataOutputStream fos=new FSDataOutputStream(outputStream,statistics){
            @Override
            public void close()throws IOException{
                super.close();
                if (!client.isConnected()) {
                    throw new FTPException("Client not connected");
                }
                boolean cmdCompleted=client.completePendingCommand();
                disconnect(client);
                if (!cmdCompleted) {
                    throw new FTPException("Could not complete transfer,Reply Code - "+client.getReplyCode());
                }
            }
        };
        return fos;
    }
    @Override
    public FSDataOutputStream append(Path f,int bufferSize,Progressable progressable)throws IOException{
        throw new UnsupportedEncodingException("Append is not supported by FTPFileSystem");
    }
    private boolean exists(FTPClient client,Path file)throws IOException{
        try {
            getFileStatus(client,file);
            return true;
        }catch (FileNotFoundException e){
            return false;
        }
    }
    @Override
    public boolean delete(Path file,boolean recursive)throws IOException{
        FTPClient client = connect();
        try {
            boolean success=delete(client,file,recursive);
            return success;
        }finally {
            disconnect(client);
        }
    }
    private boolean delete(FTPClient client,Path file,boolean recursive)throws IOException{
        Path workDir = new Path(client.printWorkingDirectory());
        Path absolute = makeAbsolute(workDir, file);
        String pathName = absolute.toUri().getPath();
        try {
            FileStatus fileStat=getFileStatus(client,absolute);
            if (fileStat.isFile()) {
                return client.deleteFile(pathName);
            }
        }catch (FileNotFoundException e){
            return false;
        }
        FileStatus[] dirEntries=listStatus(client,absolute);
        if (dirEntries != null && dirEntries.length > 0 && !(recursive)) {
            throw new IOException("Directory: "+file+" is not empty");
        }
        for (FileStatus dirEntry : dirEntries) {
            delete(client,new Path(absolute,dirEntry.getPath()),recursive);
        }
        return client.removeDirectory(pathName);
    }
    @VisibleForTesting
    FsAction getFsAction(int accessGroup, FTPFile ftpFile){
        FsAction action=FsAction.NONE;
        if (ftpFile.hasPermission(accessGroup, FTPFile.READ_PERMISSION)) {
            action=action.or(FsAction.READ);
        }
        if (ftpFile.hasPermission(accessGroup, FTPFile.WRITE_PERMISSION)) {
            action=action.or(FsAction.WRITE);
        }
        if (ftpFile.hasPermission(accessGroup, FTPFile.EXECUTE_PERMISSION)) {
            action=action.or(FsAction.EXECUTE);
        }
        return action;
    }
    private FsPermission getPermission(FTPFile ftpFile){
        FsAction user,group,others;
        user=getFsAction(FTPFile.USER_ACCESS,ftpFile);
        group=getFsAction(FTPFile.GROUP_ACCESS,ftpFile);
        others=getFsAction(FTPFile.WORLD_ACCESS,ftpFile);
        return new FsPermission(user,group,others);
    }
    @Override
    public URI getUri(){
        return uri;
    }
    @Override
    public FileStatus[] listStatus(Path file)throws IOException{
        FTPClient client = connect();
        try {
            FileStatus[] status=listStatus(client,file);
            return status;
        }finally {
            disconnect(client);
        }
    }
    private FileStatus[] listStatus(FTPClient client,Path file)throws IOException{
        Path workDir = new Path(client.printWorkingDirectory());
        Path absolute = makeAbsolute(workDir, file);
        FileStatus fileStat=getFileStatus(client,absolute);
        if (fileStat.isFile()) {
            return new FileStatus[]{fileStat};
        }
        FTPFile[] ftpFiles = client.listFiles(absolute.toUri().getPath());
        FileStatus[] fileStats = new FileStatus[ftpFiles.length];
        for (int i = 0; i < ftpFiles.length; i++) {
            fileStats[i]=getFileStatus(ftpFiles[i],absolute);
        }
        return fileStats;
    }
    @Override
    public FileStatus getFileStatus(Path file)throws IOException{
        FTPClient client = connect();
        try {
            FileStatus status=getFileStatus(client,file);
            return status;
        }finally {
            disconnect(client);
        }
    }
    private FileStatus getFileStatus(FTPClient client,Path file)throws IOException{
        FileStatus fileStat=null;
        Path workDir = new Path(client.printWorkingDirectory());
        Path absolute = makeAbsolute(workDir, file);
        Path parentPath = absolute.getParent();
        if (parentPath == null) {
            long length=-1;
            boolean isDir=true;
            int blockReplication=1;
            long blockSize=DEFAULT_BLOCK_SIZE;
            long modTime=-1;
            Path root = new Path("/");
            return new FileStatus(length,isDir,blockReplication,blockSize,modTime,this.makeQualified(root));
        }
        String pathName = parentPath.toUri().getPath();
        FTPFile[] ftpFiles = client.listFiles(pathName);
        if (ftpFiles != null) {
            for (FTPFile ftpFile : ftpFiles) {
                if (ftpFile.getName().equals(file.getName())) {
                    fileStat=getFileStatus(ftpFile,parentPath);
                    break;
                }
            }
            if (fileStat == null) {
                throw new FileNotFoundException("File "+file+" does not exist.");
            }
        }else {
            throw new FileNotFoundException("File "+file+" does not exist");
        }
        return fileStat;
    }
    private FileStatus getFileStatus(FTPFile ftpFile,Path parentPath){
        long length = ftpFile.getSize();
        boolean isDir = ftpFile.isDirectory();
        int blockReplication=1;
        long blockSize=DEFAULT_BLOCK_SIZE;
        long modTime=ftpFile.getTimestamp().getTimeInMillis();
        long accessTime=0;
        FsPermission permission = getPermission(ftpFile);
        String user = ftpFile.getUser();
        String group = ftpFile.getGroup();
        Path filePath = new Path(parentPath, ftpFile.getName());
        return new FileStatus(length,isDir,blockReplication,blockSize,modTime,accessTime,permission,user,group,this.makeQualified(filePath));
    }
    @Override
    public boolean mkdirs(Path file,FsPermission permission)throws IOException{
        FTPClient client = connect();
        try {
            boolean success=mkdirs(client,file,permission);
            return success;
        }finally {
            disconnect(client);
        }
    }
    private boolean mkdirs(FTPClient client,Path file,FsPermission permission)throws IOException{
        boolean created=true;
        Path workDir = new Path(client.printWorkingDirectory());
        Path absolute = makeAbsolute(workDir, file);
        String pathName = absolute.getName();
        if (!exists(client, absolute)) {
            Path parent = absolute.getParent();
            created=(parent==null||mkdirs(client,parent,FsPermission.getDirDefault()));
            if (created) {
                String parentDir = parent.toUri().getPath();
                client.changeWorkingDirectory(parentDir);
                created=created&&client.makeDirectory(pathName);
            }
        } else if (isFile(client, absolute)) {
            throw new ParentNotDirectoryException(String.format("Can't make directory for path %s since it is a file.",absolute));
        }
        return created;
    }
    private boolean isFile(FTPClient client,Path file){
        try {
            return getFileStatus(client,file).isFile();
        }catch (FileNotFoundException e){
            return false;
        }catch (IOException e){
            throw new FTPException("File check filed",e);
        }
    }
    @Override
    public boolean rename(Path src,Path dst)throws IOException{
        FTPClient client = connect();
        try {
            boolean success=rename(client,src,dst);
            return success;
        }finally {
            disconnect(client);
        }
    }
    private boolean isParentOf(Path parent,Path child){
        URI parentUri = parent.toUri();
        String parentPath = parentUri.getPath();
        if (!parentPath.endsWith("/")) {
            parentPath+="/";
        }
        URI childUri = child.toUri();
        String childPath = childUri.getPath();
        return childPath.startsWith(parentPath);
    }
    @SuppressWarnings("deprecation")
    private boolean rename(FTPClient client,Path src,Path dst)throws IOException{
        Path workDir = new Path(client.printWorkingDirectory());
        Path absoluteSrc = makeAbsolute(workDir, src);
        Path absoluteDst = makeAbsolute(workDir, dst);
        if (!exists(client, absoluteSrc)) {
            throw new FileNotFoundException("Source path "+src+" does not exist");
        }
        if (isDirectory(absoluteDst)) {
            absoluteDst=new Path(absoluteDst,absoluteSrc.getName());
        }
        if (exists(client, absoluteDst)) {
            throw new FileAlreadyExistsException("Destination path "+dst+" already exist");
        }
        String parentSrc = absoluteSrc.getParent().toUri().toString();
        String parentDst = absoluteDst.getParent().toUri().toString();
        if (isParentOf(absoluteSrc, absoluteDst)) {
            throw new IOException("Cannot rename "+absoluteSrc+" under itself: "+absoluteDst);
        }
        if (!parentSrc.equals(parentDst)) {
            throw new IOException("Cannot rename source: "+absoluteSrc+" to "
            +absoluteDst+" -"+E_SAME_DIRECTORY_ONLY);
        }
        String from = absoluteSrc.getName();
        String to = absoluteDst.getName();
        client.changeWorkingDirectory(parentSrc);
        boolean renamed=client.rename(from,to);
        return renamed;
    }
    @Override
    public Path getWorkingDirectory(){
        return getHomeDirectory();
    }
    @Override
    public Path getHomeDirectory(){
        FTPClient client=null;
        try {
            client=connect();
            Path homeDir = new Path(client.printWorkingDirectory());
            return homeDir;
        }catch (IOException e){
            throw new FTPException("Failed to get home directory",e);
        }finally {
            try {
                disconnect(client);
            }catch (IOException e){
                throw new FTPException("Failed to disconnect",e);
            }
        }
    }
    @Override
    public void setWorkingDirectory(Path newDir){

    }
}
