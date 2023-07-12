package org.apache.hadoop.util;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

/**
 * A helper class for getting build-info for the java-vm
 */
@InterfaceAudience.LimitedPrivate({"HBase"})
@InterfaceStability.Unstable
public class PlatformName {
    /**
     * the complete platform name to identify the platform as per the java-vm
     */
    public static final String PLATFORM_NAME=(System.getProperty("os.name").startsWith("Windows")?System.getenv("os"):System.getProperty("os.name"))
            +"-"+System.getProperty("os.arch")
            +"-"+System.getProperty("sun.arch.data.model");
    /**
     * the java vendor name used in this platform
     */
    public static final String JAVA_VENDOR_NAME=System.getProperty("java.vendor");
    /**
     * A public static variable to indicate the current java vendor is IBM java or not
     */
    public static final boolean IBM_JAVA=JAVA_VENDOR_NAME.contains("IBM");

    public static void main(String[] args) {
        System.out.println(PLATFORM_NAME);
    }
}
