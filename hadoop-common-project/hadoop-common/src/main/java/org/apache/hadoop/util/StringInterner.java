package org.apache.hadoop.util;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class StringInterner {
    private static final Interner<String > STRONG_INTERNER= Interners.newStrongInterner();

    public static String strongIntern(String sample){
        if (sample == null) {
            return null;
        }
        return STRONG_INTERNER.intern(sample);
    }

    public static String weakIntern(String sample){
        if (sample == null) {
            return null;
        }
        return sample.intern();
    }

    public static String[] internStringInArray(String[] strings){
        for (int i = 0; i < strings.length; i++) {
            strings[i]=weakIntern(strings[i]);
        }
        return strings;
    }
}
