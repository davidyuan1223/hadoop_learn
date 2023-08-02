package org.apache.hadoop.fs.impl;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StreamCapabilities;

import java.io.InputStream;
import java.io.OutputStream;

import static org.apache.hadoop.fs.StreamCapabilities.HFLUSH;
import static org.apache.hadoop.fs.StreamCapabilities.HSYNC;
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class StoreImplementationUtils {
    private StoreImplementationUtils(){}

    public static boolean isProbeForSyncable(String capability){
        return capability.equalsIgnoreCase(HSYNC)
                || capability.equalsIgnoreCase(HFLUSH);
    }

    static boolean objectHasCapability(Object object,String capability){
        if (object instanceof StreamCapabilities) {
            return ((StreamCapabilities)object).hasCapability(capability);
        }
        return false;
    }

    public static boolean hasCapability(OutputStream out,String capability){
        return objectHasCapability(out,capability);
    }

    public static boolean hasCapability(InputStream in,String capability){
        return objectHasCapability(in,capability);
    }
}
