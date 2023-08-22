package org.apache.hadoop.fs.permission;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.util.regex.Pattern;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RawParser extends PermissionParser{
    private static Pattern rawOctalPattern =
            Pattern.compile("^\\s*([01]?)([0-7]{3})\\s*$");
    private static Pattern rawNormalPattern =
            Pattern.compile("\\G\\s*([ugoa]*)([+=-]+)([rwxt]*)([,\\s]*)\\s*");

    private short permission;

    public RawParser(String modeStr)throws IllegalArgumentException{
        super(modeStr,rawNormalPattern,rawOctalPattern);
        permission=(short) combineModes(0,false);
    }

    public short getPermission() {
        return permission;
    }
}
