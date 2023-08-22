package org.apache.hadoop.fs.permission;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;

import java.util.regex.Pattern;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ChmodParser extends PermissionParser{
    private static Pattern chmodOctalPattern=
            Pattern.compile("^\\s*[+]?([01]?)([0-7]{3})\\s*$");

    private static Pattern chmodNormalPattern=
            Pattern.compile("\\G\\s*([ugoa]*)([+=-]+)([rwxXt]+)([,\\s]*)\\s*");

    public ChmodParser(String modeStr)throws IllegalArgumentException{
        super(modeStr,chmodNormalPattern,chmodOctalPattern);
    }

    public short applyNewPermission(FileStatus file){
        FsPermission perms = file.getPermission();
        int existing=perms.toShort();
        boolean execOk=file.isDirectory() || (existing&0111)!=0;
        return (short) combineModes(existing,execOk);
    }
}
