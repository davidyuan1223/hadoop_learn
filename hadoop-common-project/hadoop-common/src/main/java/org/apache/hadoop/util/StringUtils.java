package org.apache.hadoop.util;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.util.regex.Pattern;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class StringUtils {
    public static final int SHUTDOWN_HOOK_PRIORITY=0;

    public static final Pattern SHELL_ENV_VAR_PATTERN=Pattern.compile("\\$([A-Za-z_]{1}[A-Za-z0-9]*)");

    public static final Pattern WIN_ENV_VAR_PATTERN=Pattern.compile("%(.*?)%");

    public static final Pattern ENV_VAR_PATTERN=Shell

    public static String join(CharSequence separator, String[] strings) {
        StringBuilder sb = new StringBuilder();
        boolean first=true;
        for (String string : strings) {
            if (first) {
                first=false;
            }else {
                sb.append(separator);
            }
            sb.append(string);
        }
        return sb.toString();
    }
}
