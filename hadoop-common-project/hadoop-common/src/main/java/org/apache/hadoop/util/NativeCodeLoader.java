package org.apache.hadoop.util;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class NativeCodeLoader {
    private static final Logger LOG= LoggerFactory.getLogger(NativeCodeLoader.class);
    private static boolean nativeCodeLoaded=false;
    static {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Trying to load the custom-built native hadoop library...");
        }
        try {
            System.loadLibrary("hadoop");
            LOG.debug("Loaded the native-hadoop library");
            nativeCodeLoaded=true;
        }catch (Throwable t){
            if (LOG.isDebugEnabled()) {
                LOG.debug("Failed to load native-hadoop with error: "+t);
                LOG.debug("java.library.path="+System.getProperty("java.library.path"));
            }
        }
        if (!nativeCodeLoaded) {
            LOG.warn("Unable to load native-hadoop library for your platform..." +
                    "using builtin-java classes where applicable");
        }
    }
    private NativeCodeLoader(){}
    public static boolean isNativeCodeLoaded(){
        return nativeCodeLoaded;
    }
    public static native boolean buildSupportsIsal();
    public static native boolean buildSupportsZstd();
    public static native boolean buildSupportsOpenssl();
    public static native String getLibraryName();
}
