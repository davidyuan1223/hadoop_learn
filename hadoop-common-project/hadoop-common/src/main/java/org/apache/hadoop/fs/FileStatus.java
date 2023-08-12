package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/24
 **/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FileStatus implements Writable,Comparable<Object>, Serializable, ObjectInputValidation {
    private static final long serialVersionUID=0x13caeae8;
    private Path path;
    private long length;
    private Boolean isdir;
    private short block_replication;
    private long blocksize;
    private long modification_time;
    private long access_time;
    private FsPermission permission;
    private String owner;
    private String group;
    private Path symlink;
    private Set<AttrFlags> attr;
    public static final Set<AttrFlags> NONE= Collections.emptySet();

    public static Set<AttrFlags> attributes(boolean acl,boolean crypt,boolean ec,boolean sn){
        if (!(acl || crypt || ec || sn)) {
            return NONE;
        }
        EnumSet<AttrFlags> ret = EnumSet.noneOf(AttrFlags.class);
        if (acl) {
            ret.add(AttrFlags.HAS_ACL);
        }
        if (crypt) {
            ret.add(AttrFlags.HAS_CRYPT);
        }
        if (ec) {
            ret.add(AttrFlags.HAS_EC);
        }
        if (sn) {
            ret.add(AttrFlags.SNAPSHOT_ENABLED);
        }
        return ret;
    }
    public FileStatus(){
        this(0,false,0,0,0,0,null,null,null,null);
    }

    public FileStatus(long length,boolean isdir,int block_replication,long blocksize,
                      long modification_time,Path path){
        this(length,isdir,block_replication,block_replication,modification_time,0,null,null,null,path);
    }
    public FileStatus(long length, boolean isdir,
                      int block_replication,
                      long blocksize, long modification_time, long access_time,
                      FsPermission permission, String owner, String group,
                      Path path) {
        this(length, isdir, block_replication, blocksize, modification_time,
                access_time, permission, owner, group, null, path);
    }

    public FileStatus(long length, boolean isdir,
                      int block_replication,
                      long blocksize, long modification_time, long access_time,
                      FsPermission permission, String owner, String group,
                      Path symlink,
                      Path path) {
        this(length, isdir, block_replication, blocksize, modification_time,
                access_time, permission, owner, group, symlink, path,
                false, false, false);
    }

    public FileStatus(long length, boolean isdir, int block_replication,
                      long blocksize, long modification_time, long access_time,
                      FsPermission permission, String owner, String group, Path symlink,
                      Path path, boolean hasAcl, boolean isEncrypted, boolean isErasureCoded) {
        this(length, isdir, block_replication, blocksize, modification_time,
                access_time, permission, owner, group, symlink, path,
                attributes(hasAcl, isEncrypted, isErasureCoded, false));
    }

    public FileStatus(long length,boolean isdir,int block_replication,long blocksize,
                      long modification_time,long access_time,FsPermission permission,
                      String owner,String group,Path symlink,Path path,Set<AttrFlags> attr){
        this.length=length;
        this.isdir=isdir;
        this.block_replication=(short) block_replication;
        this.blocksize=blocksize;
        this.modification_time=modification_time;
        this.access_time=access_time;
        if (permission != null) {
            this.permission=permission;
        } else if (isdir) {
            this.permission=FsPermission.getDirDefault();
        } else if (symlink != null) {
            this.permission=FsPermission.getDefault();
        }else {
            this.permission=FsPermission.getFileDefault();
        }
        this.owner=owner==null?"":owner;
        this.group=group==null?"":group;
        this.symlink=symlink;
        this.path=path;
        this.attr=attr;
        assert !isdir || symlink == null;
    }

    public FileStatus(FileStatus other) throws IOException {
        // It's important to call the getters here instead of directly accessing the
        // members.  Subclasses like ViewFsFileStatus can override the getters.
        this(other.getLen(), other.isDirectory(), other.getReplication(),
                other.getBlockSize(), other.getModificationTime(), other.getAccessTime(),
                other.getPermission(), other.getOwner(), other.getGroup(),
                (other.isSymlink() ? other.getSymlink() : null),
                other.getPath());
    }

    public long getLen() {
        return length;
    }
    public boolean isFile(){
        return !isDirectory()&&!isSymlink();
    }
    public boolean isDirectory(){
        return isdir;
    }
    @Deprecated
    public final boolean isDir(){
        return isDirectory();
    }
    public boolean isSymlink(){
        return symlink!=null;
    }

    public long getBlocksize() {
        return blocksize;
    }

    public short getReplication() {
        return block_replication;
    }

    public long getModificationTime() {
        return modification_time;
    }

    public long getAccessTime() {
        return access_time;
    }

    public FsPermission getPermission() {
        return permission;
    }
    public boolean hasAcl() {
        return attr.contains(AttrFlags.HAS_ACL);
    }

    /**
     * Tell whether the underlying file or directory is encrypted or not.
     *
     * @return true if the underlying file is encrypted.
     */
    public boolean isEncrypted() {
        return attr.contains(AttrFlags.HAS_CRYPT);
    }

    /**
     * Tell whether the underlying file or directory is erasure coded or not.
     *
     * @return true if the underlying file or directory is erasure coded.
     */
    public boolean isErasureCoded() {
        return attr.contains(AttrFlags.HAS_EC);
    }

    /**
     * Check if directory is Snapshot enabled or not.
     *
     * @return true if directory is snapshot enabled
     */
    public boolean isSnapshotEnabled() {
        return attr.contains(AttrFlags.SNAPSHOT_ENABLED);
    }

    /**
     * Get the owner of the file.
     * @return owner of the file. The string could be empty if there is no
     *         notion of owner of a file in a filesystem or if it could not
     *         be determined (rare).
     */
    public String getOwner() {
        return owner;
    }

    /**
     * Get the group associated with the file.
     * @return group for the file. The string could be empty if there is no
     *         notion of group of a file in a filesystem or if it could not
     *         be determined (rare).
     */
    public String getGroup() {
        return group;
    }

    public Path getPath() {
        return path;
    }

    public void setPath(final Path p) {
        path = p;
    }

    protected void setPermission(FsPermission permission) {
        this.permission = (permission == null) ?
                FsPermission.getFileDefault() : permission;
    }

    /**
     * Sets owner.
     * @param owner if it is null, default value is set
     */
    protected void setOwner(String owner) {
        this.owner = (owner == null) ? "" : owner;
    }

    /**
     * Sets group.
     * @param group if it is null, default value is set
     */
    protected void setGroup(String group) {
        this.group = (group == null) ? "" :  group;
    }

    /**
     * @return The contents of the symbolic link.
     *
     * @throws IOException raised on errors performing I/O.
     */
    public Path getSymlink() throws IOException {
        if (!isSymlink()) {
            throw new IOException("Path " + path + " is not a symbolic link");
        }
        return symlink;
    }

    public void setSymlink(final Path p) {
        symlink = p;
    }

    /**
     * Compare this FileStatus to another FileStatus
     * @param   o the FileStatus to be compared.
     * @return  a negative integer, zero, or a positive integer as this object
     *   is less than, equal to, or greater than the specified object.
     */
    public int compareTo(FileStatus o) {
        return this.getPath().compareTo(o.getPath());
    }

    /**
     * Compare this FileStatus to another FileStatus.
     * This method was added back by HADOOP-14683 to keep binary compatibility.
     *
     * @param   o the FileStatus to be compared.
     * @return  a negative integer, zero, or a positive integer as this object
     *   is less than, equal to, or greater than the specified object.
     * @throws ClassCastException if the specified object is not FileStatus
     */
    @Override
    public int compareTo(Object o) {
        FileStatus other = (FileStatus) o;
        return compareTo(other);
    }

    /** Compare if this object is equal to another object
     * @param   o the object to be compared.
     * @return  true if two file status has the same path name; false if not.
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof FileStatus)) {
            return false;
        }
        if (this == o) {
            return true;
        }
        FileStatus other = (FileStatus)o;
        return this.getPath().equals(other.getPath());
    }

    /**
     * Returns a hash code value for the object, which is defined as
     * the hash code of the path name.
     *
     * @return  a hash code value for the path name.
     */
    @Override
    public int hashCode() {
        return getPath().hashCode();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName())
                .append("{")
                .append("path=" + path)
                .append("; isDirectory=" + isdir);
        if(!isDirectory()){
            sb.append("; length=" + length)
                    .append("; replication=" + block_replication)
                    .append("; blocksize=" + blocksize);
        }
        sb.append("; modification_time=" + modification_time)
                .append("; access_time=" + access_time)
                .append("; owner=" + owner)
                .append("; group=" + group)
                .append("; permission=" + permission)
                .append("; isSymlink=" + isSymlink());
        if(isSymlink()) {
            try {
                sb.append("; symlink=" + getSymlink());
            } catch (IOException e) {
                throw new RuntimeException("Unexpected exception", e);
            }
        }
        sb.append("; hasAcl=" + hasAcl())
                .append("; isEncrypted=" + isEncrypted())
                .append("; isErasureCoded=" + isErasureCoded())
                .append("}");
        return sb.toString();
    }

    /**
     * Read instance encoded as protobuf from stream.
     * @param in Input stream
     * @see PBHelper#convert(FileStatus)
     * @deprecated Use the {@link PBHelper} and protobuf serialization directly.
     */
    @Override
    @Deprecated
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        if (size < 0) {
            throw new IOException("Can't read FileStatusProto with negative " +
                    "size of " + size);
        }
        byte[] buf = new byte[size];
        in.readFully(buf);
        FileStatusProto proto = FileStatusProto.parseFrom(buf);
        FileStatus other = PBHelper.convert(proto);
        isdir = other.isDirectory();
        length = other.getLen();
        block_replication = other.getReplication();
        blocksize = other.getBlockSize();
        modification_time = other.getModificationTime();
        access_time = other.getAccessTime();
        setPermission(other.getPermission());
        setOwner(other.getOwner());
        setGroup(other.getGroup());
        setSymlink((other.isSymlink() ? other.getSymlink() : null));
        setPath(other.getPath());
        attr = attributes(other.hasAcl(), other.isEncrypted(),
                other.isErasureCoded(), other.isSnapshotEnabled());
        assert !(isDirectory() && isSymlink()) : "A directory cannot be a symlink";
    }

    /**
     * Write instance encoded as protobuf to stream.
     * @param out Output stream
     * @see PBHelper#convert(FileStatus)
     * @deprecated Use the {@link PBHelper} and protobuf serialization directly.
     */
    @Override
    @Deprecated
    public void write(DataOutput out) throws IOException {
        FileStatusProto proto = PBHelper.convert(this);
        int size = proto.getSerializedSize();
        out.writeInt(size);
        out.write(proto.toByteArray());
    }

    @Override
    public void validateObject() throws InvalidObjectException {
        if (null == path) {
            throw new InvalidObjectException("No Path in deserialized FileStatus");
        }
        if (null == isdir) {
            throw new InvalidObjectException("No type in deserialized FileStatus");
        }
    }

    public enum AttrFlags{
        HAS_ACL,
        HAS_CRYPT,
        HAS_EC,
        SNAPSHOT_ENABLED,
    }
}
