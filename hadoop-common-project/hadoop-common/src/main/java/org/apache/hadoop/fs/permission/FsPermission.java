package org.apache.hadoop.fs.permission;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

@InterfaceAudience.Public
@InterfaceStability.Stable
/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/29
 **/
public class FsPermission implements Writable, Serializable, ObjectInputValidation {
    private static final Logger LOG= LoggerFactory.getLogger(FsPermission.class);
    private static final long serialVersionUID=0x2fe08564;
    static final WritableFactory FACTORY=new WritableFactory() {
        @Override
        public Writable newInstance() {
            return new FsPermission();
        }
    };
    static {
        WritableFactories.setFactory(FsPermission.class,FACTORY);
        WritableFactories.setFactory(ImmutableFsPermission.class,FACTORY);
    }
    public static final int MAX_PERMISSION_LENGTH=10;
    public static FsPermission createImmutable(short permission){
        return new ImmutableFsPermission(permission);
    }
    private FsAction useraction=null;
    private FsAction groupaction=null;
    private FsAction otheraction=null;
    private Boolean stickyBit=false;

    private FsPermission(){}

    public FsPermission(FsAction u,FsAction g,FsAction o){
        this(u,g,o,false);
    }
    public FsPermission(FsAction u,FsAction g,FsAction o,boolean sb){
        set(u,g,o,sb);
    }

    public FsPermission(short mode){
        fromShort(mode);
    }
    public FsPermission(int mode){
        this((short) (mode&01777));
    }

    public FsPermission(FsPermission other){
        this.useraction=other.useraction;
        this.groupaction=other.groupaction;
        this.otheraction=other.otheraction;
        this.stickyBit=other.stickyBit;
    }
    public FsPermission(String mode){
        this(new RawParser(mode).getPermission());
    }

    public FsAction getUserAction() {
        return useraction;
    }

    public FsAction getGroupAction() {
        return groupaction;
    }

    public FsAction getOtherAction() {
        return otheraction;
    }

    private void set(FsAction u,FsAction g,FsAction o,boolean sb){
        useraction=u;
        groupaction=g;
        otheraction=o;
        stickyBit=sb;
    }

    public void fromShort(short n){
        FsAction[] v=FSACTION_VALUES;
        set(v[(n>>>6)&7],v[(n>>>3) &7],v[n&7],(((n>>>9)&1)==1));
    }

    @Override
    @Deprecated
    public void writer(DataOutput out) throws IOException {
        out.writeShort(toShort());
    }

    @Override
    @Deprecated
    public void readFields(DataInput in) throws IOException {
        fromShort(in.readShort());
    }
    public FsPermission getMasked(){
        return null;
    }
    public FsPermission getUnmasked(){
        return null;
    }
    public static FsPermission read(DataInput in)throws IOException{
        FsPermission p = new FsPermission();
        p.fromShort(in.readShort());
        return p;
    }
    public short toShort(){
        int s= (stickyBit ? 1<<9 :0) |
                (useraction.ordinal() << 6) |
                (groupaction.ordinal() << 3) |
                otheraction.ordinal();
        return (short) s;
    }
    @Deprecated
    public short toExtendedShort(){
        return toShort();
    }

    public short toOctal(){
        int n = this.toShort();
        int octal = (n>>>9&1)*1000 + (n>>>6&7)*100 + (n>>>3&7)*10 + (n&7);
        return (short)octal;
    }
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FsPermission) {
            FsPermission that = (FsPermission)obj;
            return this.useraction == that.useraction
                    && this.groupaction == that.groupaction
                    && this.otheraction == that.otheraction
                    && this.stickyBit.booleanValue() == that.stickyBit.booleanValue();
        }
        return false;
    }

    @Override
    public int hashCode() {return toShort();}

    @Override
    public String toString() {
        String str = useraction.SYMBOL + groupaction.SYMBOL + otheraction.SYMBOL;
        if(stickyBit) {
            StringBuilder str2 = new StringBuilder(str);
            str2.replace(str2.length() - 1, str2.length(),
                    otheraction.implies(FsAction.EXECUTE) ? "t" : "T");
            str = str2.toString();
        }

        return str;
    }
    public FsPermission applyUMask(FsPermission umask){
        return new FsPermission(useraction.and(umask.useraction.not()),
                groupaction.and(umask.groupaction.not()),
                otheraction.and(umask.otheraction.not()));
    }
    public static final String UMASK_LABEL=
            CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY;
    public static final int DEFAULT_UMASK=
            CommonConfigurationKeys.FS_PERMISSIONS_UMASK_DEFAULT;
    private static final FsAction[] FSACTION_VALUES=FsAction.values();

    public static FsPermission getUMask(Configuration conf){
        int umask=DEFAULT_UMASK;
        if (conf != null) {
            String confUMask = conf.get(UMASK_LABEL);
            try {
                if (confUMask != null) {
                    umask=new UmaskParser(confUMask).getUMask();
                }
            }catch (IllegalArgumentException e){
                String type = e instanceof NumberFormatException ? "decimal" : "octal or symbolic";
                String error = "Unable to parse configuration " + UMASK_LABEL + " with value " + confUMask + " as " + type + " umask. ";
                LOG.warn(error);
                throw new IllegalArgumentException(error);
            }
        }
        return new FsPermission((short) umask);
    }

    public Boolean getStickyBit() {
        return stickyBit;
    }

    @Deprecated
    public boolean getAclBit(){return false;}
    @Deprecated
    public boolean getEncryptedBit(){return false;}
    @Deprecated
    public boolean getErasureCodeBit(){return false;}
    public static void setUMask(Configuration conf,FsPermission umask){
        conf.set(UMASK_LABEL,String.format("%1$03o",umask.toShort()));
    }
    public static FsPermission getDefault() {
        return new FsPermission((short)00777);
    }

    /**
     * Get the default permission for directory.
     *
     * @return DirDefault FsPermission.
     */
    public static FsPermission getDirDefault() {
        return new FsPermission((short)00777);
    }

    /**
     * Get the default permission for file.
     *
     * @return FileDefault FsPermission.
     */
    public static FsPermission getFileDefault() {
        return new FsPermission((short)00666);
    }

    /**
     * Get the default permission for cache pools.
     *
     * @return CachePoolDefault FsPermission.
     */
    public static FsPermission getCachePoolDefault() {
        return new FsPermission((short)00755);
    }

    /**
     * Create a FsPermission from a Unix symbolic permission string
     * @param unixSymbolicPermission e.g. "-rw-rw-rw-"
     * @return FsPermission.
     */
    public static FsPermission valueOf(String unixSymbolicPermission) {
        if (unixSymbolicPermission == null) {
            return null;
        }
        else if (unixSymbolicPermission.length() != MAX_PERMISSION_LENGTH) {
            throw new IllegalArgumentException(String.format(
                    "length != %d(unixSymbolicPermission=%s)", MAX_PERMISSION_LENGTH,
                    unixSymbolicPermission));
        }

        int n = 0;
        for(int i = 1; i < unixSymbolicPermission.length(); i++) {
            n = n << 1;
            char c = unixSymbolicPermission.charAt(i);
            n += (c == '-' || c == 'T' || c == 'S') ? 0: 1;
        }

        // Add sticky bit value if set
        if(unixSymbolicPermission.charAt(9) == 't' ||
                unixSymbolicPermission.charAt(9) == 'T')
            n += 01000;

        return new FsPermission((short)n);
    }
    @Override
    public void validateObject() throws InvalidObjectException {
        if (null == useraction || null == groupaction || null == otheraction) {
            throw new InvalidObjectException("Invalid mode in FsPermission");
        }
        if (null == stickyBit) {
            throw new InvalidObjectException("No sticky bit in FsPermission");
        }
    }
    private static class ImmutableFsPermission extends FsPermission{
        private static final long serialVersionUID=0x1bab54bd;
        public ImmutableFsPermission(short permission){
            super(permission);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            throw new UnsupportedOperationException();
        }
    }
}
