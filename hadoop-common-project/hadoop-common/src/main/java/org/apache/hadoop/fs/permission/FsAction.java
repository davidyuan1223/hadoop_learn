package org.apache.hadoop.fs.permission;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Stable
public enum FsAction {
    NONE("---"),
    EXECUTE("--x"),
    WRITE("-w-"),
    WRITE_EXECUTE("-wx"),
    READ("r--"),
    READ_EXECUTE("r-x"),
    READ_WRITE("rw-"),
    ALL("rwx");
    private final static FsAction[] vals=values();
    public final String SYMBOL;
    private FsAction(String s){
        SYMBOL=s;
    }

    public boolean implies(FsAction that){
        if (that != null) {
            return (ordinal() & that.ordinal())==that.ordinal();
        }
        return false;
    }
    public FsAction and(FsAction that){
        return vals[ordinal() & that.ordinal()];
    }
    public FsAction or(FsAction that){
        return vals[ordinal() | that.ordinal()];
    }
    public FsAction not(){
        return vals[7-ordinal()];
    }
    public static FsAction getFsAction(String permission){
        for (FsAction fsAction : vals) {
            if (fsAction.SYMBOL.equals(permission)) {
                return fsAction;
            }
        }
        return null;
    }
}
