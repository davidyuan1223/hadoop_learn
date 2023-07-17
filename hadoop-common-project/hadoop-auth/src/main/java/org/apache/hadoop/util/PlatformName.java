package org.apache.hadoop.util;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/16
 **/
@InterfaceAudience.LimitedPrivate({"HBase"})
@InterfaceStability.Unstable
public class PlatformName {
    public static final String PLATFORM_NAME=
            (System.getProperty("os.name").startsWith("Windows")?
                    System.getenv("os"):System.getProperty("os.name"))
            +"-"+System.getProperty("os.arch")
            +"-"+System.getProperty("sun.arch.data.model");
    public static final String JAVA_VERDOR_NAME=System.getProperty("java.vendor");
    public static final boolean IBM_JAVA=JAVA_VERDOR_NAME.contains("IBM")
            &&hasIbmTechnologyEditionModules();

    private static boolean hasIbmTechnologyEditionModules() {
        return Arrays.asList(
                "com.ibm.security.auth.module.JAASLoginModule",
                "com.ibm.security.auth.module.Win64LoginModule",
                "com.ibm.security.auth.module.NTLoginModule",
                "com.ibm.security.auth.module.AIX64LoginModule",
                "com.ibm.security.auth.module.LinuxLoginModule",
                "com.ibm.security.auth.module.Kerb5LoginModule"
        ).stream().anyMatch((module)->isSystemClassAvailable(module));
    }

    private static boolean isSystemClassAvailable(String className) {
        return AccessController.doPrivileged((PrivilegedAction<Boolean>)()->{
            try {
                new SystemClassAccessor().getSystemClass(className);
                return true;
            }catch (Exception e){
                return false;
            }
        });
    }

    private static final class SystemClassAccessor extends ClassLoader{
        public Class<?> getSystemClass(String className) throws ClassNotFoundException {
            return findSystemClass(className);
        }
    }

    public static void main(String[] args) {
        System.out.println(PLATFORM_NAME);
    }
}
