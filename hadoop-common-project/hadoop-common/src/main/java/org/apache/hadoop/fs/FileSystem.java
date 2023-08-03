package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.DelegationTokenIssuer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

public abstract class FileSystem extends Configured
    implements Closeable, DelegationTokenIssuer,PathCapabilities {
    public static final String FS_DEFAULT_NAME_KEY=CommonConfigurationKeys.FS_DEFAULT_NAME_KEY;
    public static final String DEFAULT_FS=CommonConfigurationKeys.FS_DEFAULT_NAME_DEFAULT;
    @InterfaceAudience.Private
    public static final Logger LOG= LoggerFactory.getLogger(FileSystem.class);
    private static final Logger LOGGER=LoggerFactory.getLogger(FileSystem.class);
    public static final int SHUTDOWN_HOOK_PRIORITY=10;
    public static final String TRASH_PREFIX=".Trash";
    public static final String USER_HOME_PREFIX="/user";











    static final class Cache{
        private final ClientFinalizer clientFinalizer=new ClientFinalizer();
    }
    private class ClientFinalizer implements Runnable{

        @Override
        public synchronized void run() {
            try {
                closeAll(true);
            }catch (IOException e){
                LOGGER.info("FileSystem.Cache.closeAll() threw an exception:\n"+e);
            }
        }
        synchronized void closeAll(UserGroupInformation ugi)throws IOException{

        }
    }
}
