package org.apache.hadoop.security.authentication.util;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import com.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@InterfaceStability.Unstable
@InterfaceAudience.Private
public abstract class RolloverSignerSecretProvider extends SignerSecretProvider{
    @VisibleForTesting
    static Logger LOG= LoggerFactory.getLogger(RolloverSignerSecretProvider.class);
    private volatile byte[][] secrets;
    private ScheduledExecutorService scheduler;
    private boolean schedulerRunning;
    private boolean isDestroyed;

    public RolloverSignerSecretProvider(){
        schedulerRunning=false;
        isDestroyed=false;
    }
    @Override
    public void init(Properties config, ServletContext servletContext, long tokenValidity) throws Exception {
        initSecrets(generateNewSecret(),null);
        startScheduler(tokenValidity,tokenValidity);
    }

    protected abstract byte[] generateNewSecret();
    protected void initSecrets(byte[] currentSecret,byte[] previousSecret){
        secrets=new byte[][]{currentSecret,previousSecret};
    }
    protected synchronized void startScheduler(long initialDelay,long period){
        if (!schedulerRunning) {
            schedulerRunning=true;
            scheduler=Executors.newSingleThreadScheduledExecutor();
            scheduler.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    rollSecret();
                }
            },initialDelay,period, TimeUnit.MILLISECONDS);
        }
    }
    protected synchronized void rollSecret(){
        if (!isDestroyed) {
            LOG.debug("rolling secret");
            byte[] newSecret = generateNewSecret();
            secrets=new byte[][]{newSecret,secrets[0]};
        }
    }
    @Override
    public byte[] getCurrentSecret() {
        return secrets[0];
    }

    @Override
    public byte[][] getAllSecrets() {
        return secrets;
    }

    @Override
    public void destroy() {
        if (!isDestroyed) {
            isDestroyed=true;
            if (scheduler != null) {
                scheduler.shutdown();
            }
            schedulerRunning=false;
        }
        super.destroy();
    }
}
