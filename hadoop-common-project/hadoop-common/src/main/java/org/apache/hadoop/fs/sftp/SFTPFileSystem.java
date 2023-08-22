package org.apache.hadoop.fs.sftp;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;

public class SFTPFileSystem extends FileSystem {
    public static final Logger LOG= LoggerFactory.getLogger(SFTPFileSystem.class);
    private SFTPConnectionPool connectionPool;
    private URI uri;
    private final AtomicBoolean closed=new AtomicBoolean(false);
    private static final int DEFAULT_SFTP_PORT=22;
    private static final int DEFAULT_MAX_CONNECTION=5;
    private static final int DEFAULT_BUFFER_SIZE=1024*1024;
    private static final int DEFAULT_BLOCK_SIZE=4*1024;
    public static final String FS_SFTP_USER_PREFIX="fs.sftp.user.";
    public static final String FS_SFTP_PASSWORD_PREFIX="fs.sftp.password";
    public static final String FS_SFTP_HOST="fs.sftp.host";
    public static final String FS_SFTP_HOST_PORT="fs.sftp.host.port";
    public static final String FS_SFTP_KEYFILE="fs.sft.keyfile";
    public static final String FS_SFTP_CONNECTION_MAX="fs.sftp.connection.max";
    public static final String E_SAME_DIRECTORY_ONLY=
            "only same directory renames are supported";
    public static final String E_HOST_NULL="Invalid host specified";
    public static final String E_USER_NULL="No user specified for sftp connection. Expand URI or credential file";
    public static final String E_PATH_DIR="Path ^s is a directory";
    public static final String E_FILE_STATUS="Failed to get file status";
    public static final String E_FILE_NOTFOUND="File %s does not exist.";
    public static final String E_FILE_EXIST="File already exist: %s";
    public static final String E_CREATE_DIR="create(): Mkdirs failed to create: %s";
    public static final String E_DIR_CREATE_FROMFILE =
            "Can't make directory for path %s since it is a file.";
    public static final String E_MAKE_DIR_FORPATH =
            "Can't make directory for path \"%s\" under \"%s\".";
    public static final String E_DIR_NOTEMPTY="Directory: %s is not empty";
    public static final String E_FILE_CHECK_FAILED="File check failed";
    public static final String E_SPATH_NOTEXIST="Source path %s does not exist";
    public static final String E_DPATH_EXIST="Destination path %s already exist, cannot rename!";
    public static final String E_FAILED_GETHOME="Failed to get home directory";
    public static final String E_FAILED_DISCONNECT="Failed to disconnect";
    public static final String E_FS_CLOSED="FileSystem is closed";

    private void setConfigurationFromURI(URI uriInfo, Configuration conf)throws IOException{
        String host = uriInfo.getHost();
        host=(host==null)?conf.get(FS_SFTP_HOST,null):host;
        if (host == null) {
            throw new IOException(E_HOST_NULL);
        }
        conf.set(FS_SFTP_HOST,host);
        int port = uriInfo.getPort();
        port=(port==-1)?conf.getInt(FS_SFTP_HOST_PORT,DEFAULT_SFTP_PORT):port;
        conf.setInt(FS_SFTP_HOST_PORT,port);
        String userAndPasswordFromUri = uriInfo.getUserInfo();
        if (userAndPasswordFromUri != null) {
            String[] userPasswordInfo = userAndPasswordFromUri.split(":");
            String user = userPasswordInfo[0];
            user= URLDecoder.decode(user,"UTF-8");
            conf.set(FS_SFTP_USER_PREFIX+host,user);
            if (userPasswordInfo.length>1) {
                conf.set(FS_SFTP_PASSWORD_PREFIX+host+"."+user,userPasswordInfo[1]);
            }
        }
        String user = conf.get(FS_SFTP_USER_PREFIX + host);
        if (user == null || user.equals("")) {
            throw new IllegalArgumentException(E_USER_NULL);
        }
        int connectionMax = conf.getInt(FS_SFTP_CONNECTION_MAX, DEFAULT_MAX_CONNECTION);
        connectionPool=new SFTPConnectionPool(connectionMax);
    }

    private ChannelSftp connect()throws IOException{
        checkNotClosed();
        Configuration conf = getConf();
        String host = conf.get(FS_SFTP_HOST, null);
        int port = conf.getInt(FS_SFTP_HOST_PORT, DEFAULT_SFTP_PORT);
        String user = conf.get(FS_SFTP_USER_PREFIX + host, null);
        String pwd = conf.get(FS_SFTP_PASSWORD_PREFIX + host + "." + user, null);
        String keyFile = conf.get(FS_SFTP_KEYFILE, null);
        return connectionPool.connect(host, port, user, pwd, keyFile);
    }

    private void disconnect(ChannelSftp channel)throws IOException{
        connectionPool.disconnect(channel);
    }

    private Path makeAbsolute(Path workDir,Path path){
        if (path.isAbsolute()) {
            return path;
        }
        return new Path(workDir,path);
    }

    private boolean exists(ChannelSftp channel,Path file)throws IOException{
        try {
            getFileStatus(channel,file);
            return true;
        }catch (FileNotFoundException e){
            return false;
        }catch (IOException e){
            throw new IOException(E_FILE_STATUS,e);
        }
    }

    @SuppressWarnings("unchecked")
    private FileStatus getFileStatus(ChannelSftp client, Path file)throws IOException {
        FileStatus fileStatus = null;
        Path workDir;
        try {
            workDir = new Path(client.pwd());
        } catch (SftpException e) {
            throw new IOException(e);
        }
        Path absolute = makeAbsolute(workDir, file);
        Path parentPath = absolute.getParent();
        if (parentPath == null) {
            long length = -1;
            boolean isDir = true;
            long blockReplication = 1;
            long blockSize = DEFAULT_BLOCK_SIZE;
            long modTime = -1;
            Path root = new Path("/");
            return new FileStatus(length, isDir, blockReplication, blockSize, modTime, root.makeQualified(this.getUri(), this.getWorkingDirectory(client)));
        }
        String pathName = parentPath.toUri().getPath();
        Vector<ChannelSftp.LsEntry> sftpFiles;
        try {
            sftpFiles = (Vector<ChannelSftp.LsEntry>) client.ls(pathName);
        } catch (SftpException e) {
            throw new FileNotFoundException(String.format(E_FILE_NOTFOUND, file));
        }
        if (sftpFiles != null) {
            for (ChannelSftp.LsEntry sftpFile : sftpFiles) {
                if (sftpFile.getFilename().equals(file.getName())) {
                    fileStatus = getFileStatus(client, sftpFile, parentPath);
                    break;
                }
            }
            if (fileStatus == null) {
                throw new FileNotFoundException(String.format(E_FILE_NOTFOUND, file));
            }
        }else {
            throw new FileNotFoundException(String.format(E_FILE_NOTFOUND,file));
        }
        return fileStatus;
    }

    private FileStatus getFileStatus(ChannelSftp channel, ChannelSftp.LsEntry sftpFile,Path parentPath)throws IOException{
        SftpATTRS attr = sftpFile.getAttrs();
        long length = attr.getSize();
        boolean isDir = attr.isDir();
        boolean isLink = attr.isLink();
        if (isLink) {
            String link = parentPath.toUri().getPath() + "/" + sftpFile.getFilename();
            try {
                link=channel.readlink(link);
                Path linkParent = new Path("/", link);
                FileStatus fstat = getFileStatus(channel, linkParent);
                isDir=fstat.isDirectory();
                length=fstat.getLen();
            }catch (Exception e){
                throw new IOException(e);
            }
        }
        int blockReplication=1;
        long blockSize=DEFAULT_BLOCK_SIZE;
        long modTime=attr.getMTime()*1000L;
        long accessTime=attr.getATime()*1000L;
        FsPermission permission=getPermissions(sftpFile);
        String user = Integer.toString(attr.getUId());
        String group = Integer.toString(attr.getGId());
        Path filePath = new Path(parentPath, sftpFile.getFilename());
        return new FileStatus(length,isDir,blockReplication,blockSize,modTime,
                accessTime,permission,user,group,filePath.makeQualified(this.geturi(),this.getWorkingDirectory(channel)));
    }
    private FsPermission getPermissions(ChannelSftp.LsEntry sftpFile){
        return new FsPermission((short) sftpFile.getAttrs().getPermissions());
    }

    private boolean mkdirs(ChannelSftp client,Path file,FsPermission permission)throws IOException{
        boolean created=true;
        Path workDir;
        try {
            workDir=new Path(client.pwd());
        }catch (SftpException e){
            throw new IOException(e);
        }
        Path absolute = makeAbsolute(workDir, file);
        String pathName = absolute.getName();
        if (!exists(client, absolute)) {
            Path parent = absolute.getParent();
            created=(parent==null || mkdirs(client,parent,FsPermission.getDefault()));
            if (created) {
                String parentDir = parent.toUri().getPath();
                boolean succeeded=true;
                try {
                    final String previouseCwd=client.pwd();
                    client.cd(parentDir);
                    client.mkdir(pathName);
                    client.cd(previouseCwd);
                }catch (SftpException e){
                    throw new IOException(String.format(E_MAKE_DIR_FORPATH,pathName,parentDir));
                }
                created=created&succeeded;
            }
        } else if (isFile(client, absolute)) {
            throw new IOException(String.format(E_DIR_CREATE_FROMFILE,absolute));
        }
        return created;
    }

    private boolean isFile(ChannelSftp channel,Path file)throws IOException{
        try {
            return !getFileStatus(channel,file).isDirectory();
        }catch (FileNotFoundException e){
            return false;
        }catch (IOException e){
            throw new IOException(E_FILE_CHECK_FAILED,e);
        }
    }

    private boolean delete(ChannelSftp channel,Path file,boolean recursive)throws IOException{
        Path wordDir;
        try {
            wordDir=new Path(channel.pwd());
        }catch (SftpException e){
            throw new IOException(e);
        }
        Path absolute = makeAbsolute(wordDir, file);
        String pathName = absolute.toUri().getPath();
        FileStatus fileState=null;
        try {
            fileState=getFileStatus(channel,absolute);
        }catch (FileNotFoundException e){
            return false;
        }
        if (!fileState.isDirectory()) {
            boolean status=true;
            try{
                channel.rm(pathName);
            }catch (SftpException e){
                status=false;
            }
            return status;
        }else {
            boolean status=true;
            FileStatus[] dirEntries=listStatus(channel,absolute);
            if (dirEntries != null && dirEntries.length > 0) {
                if (!recursive) {
                    throw new IOException(String.format(E_DIR_NOTEMPTY,file));
                }
                for (FileStatus dirEntry : dirEntries) {
                    delete(channel, new Path(absolute, dirEntry.getPath()), recursive);
                }
            }
            try {
                channel.rmdir(pathName);
            }catch (SftpException e){
                status=false;
            }
            return status;
        }
    }

    @SuppressWarnings("unchecked")
    private FileStatus[] listStatus(ChannelSftp channel, Path file)throws IOException{
        Path workDir;
        try {
            workDir=new Path(channel.pwd());
        }catch (SftpException e){
            throw new IOException(e);
        }
        Path absolute = makeAbsolute(workDir, file);
        FileStatus fileStat = getFileStatus(channel, absolute);
        if (!fileStat.isDirectory()) {
            return new FileStatus[]{fileStat};
        }
        Vector<ChannelSftp.LsEntry> sftpFiles;
        try {
            sftpFiles=channel.ls(absolute.toUri().getPath());
        }catch (SftpException e){
            throw new IOException(e);
        }
        ArrayList<FileStatus> fileStats=new ArrayList<>();
        for (ChannelSftp.LsEntry sftpFile : sftpFiles) {
            String fname = sftpFile.getFilename();
            if (!".".equalsIgnoreCase(fname) && !"..".equalsIgnoreCase(fname)) {
                fileStats.add(getFileStatus(channel,sftpFile,absolute));
            }
        }
        return fileStats.toArray(new FileStatus[fileStats.size()]);
    }



    private void checkNotClosed()throws IOException{
        if (closed.get()) {
            throw new IOException(uri+": "+E_FS_CLOSED);
        }
    }

}
