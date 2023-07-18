package org.apache.hadoop.security.authentication.util;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import com.apache.hadoop.classification.VisibleForTesting;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.Configuration;
import javax.servlet.ServletContext;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Properties;
import java.util.Random;

@InterfaceStability.Unstable
@InterfaceAudience.Private
public class ZKSignerSecretProvider extends RolloverSignerSecretProvider{
    private static final String CONFIG_PREFIX="signer.secret.provider.zookeeper.";
    public static final String ZOOKEEPER_CONNECTION_STRING=CONFIG_PREFIX+"connection.string";
    public static final String ZOOKEEPER_PATH=CONFIG_PREFIX+"path";
    public static final String ZOOKEEPER_AUTH_TYPE=CONFIG_PREFIX+"auth.type";
    public static final String ZOOKEEPER_KERBEROS_PRINCIPAL=CONFIG_PREFIX+"kerberos.principal";
    public static final String ZOOKEEPER_KERBEROS_KEYTAB=CONFIG_PREFIX+"kerberos.keytab";
    public static final String DISCONNECT_FROM_ZOOKEEPER_ON_SHUTDOWN=CONFIG_PREFIX+"disconnect.on.shutdown";
    public static final String ZOOKEEPER_SIGNER_SECRET_PROVIDER_CURATOR_CLIENT_ATTRIBUTE=
            CONFIG_PREFIX+"curator.client";
    private static final String JAAS_LOGIN_ENTRY_NAME="ZKSignerSecretProviderClient";
    private static Logger LOG= LoggerFactory.getLogger(ZKSignerSecretProvider.class);
    private String path;
    private volatile byte[] nextSecret;
    private final Random random;
    private int zkVersion;
    private long nextRolloverDate;
    private long tokenValidity;
    private CuratorFramework client;
    private boolean shouldDisconnect;
    private static int INT_BYTES=Integer.SIZE/Byte.SIZE;
    private static int LONG_BYTES=Long.SIZE/Byte.SIZE;
    private static int DATA_VERSION=0;
    public ZKSignerSecretProvider(){
        super();
        random=new SecureRandom();
    }
    @VisibleForTesting
    public ZKSignerSecretProvider(long seed){
        super();
        random=new Random(seed);
    }

    @Override
    public void destroy() {
        if (shouldDisconnect && client != null) {
            client.close();
        }
        super.destroy();
    }

    @Override
    protected synchronized void rollSecret() {
        super.rollSecret();
        nextRolloverDate+=tokenValidity;
        byte[][] secrets = super.getAllSecrets();
        pushToZK(generateNewSecret(),secrets[0],secrets[1]);
        pullFromZK(false);
    }

    private synchronized void pushToZK(byte[] newSecret, byte[] currentSecret, byte[] previousSecret) {
        byte[] bytes = generateZKData(newSecret, currentSecret, previousSecret);
        try {
            client.setData().withVersion(zkVersion).forPath(path, bytes);
        } catch (KeeperException.BadVersionException bve) {
            LOG.debug("Unable to push to znode; another server already did it");
        } catch (Exception ex) {
            LOG.error("An unexpected exception occurred pushing data to ZooKeeper",
                    ex);
        }
    }

    @Override
    public void init(Properties config, ServletContext servletContext, long tokenValidity) throws Exception {
        Object curatorClientObj = servletContext.getAttribute(ZOOKEEPER_SIGNER_SECRET_PROVIDER_CURATOR_CLIENT_ATTRIBUTE);
        if (curatorClientObj != null && curatorClientObj instanceof CuratorFramework) {
            client= (CuratorFramework) curatorClientObj;
        }else {
            client=createCuratorClient(config);
            servletContext.setAttribute(ZOOKEEPER_SIGNER_SECRET_PROVIDER_CURATOR_CLIENT_ATTRIBUTE,client);
        }
        this.tokenValidity=tokenValidity;
        shouldDisconnect=Boolean.parseBoolean(
                config.getProperty(DISCONNECT_FROM_ZOOKEEPER_ON_SHUTDOWN,"true")
        );
        path=config.getProperty(ZOOKEEPER_PATH);
        if (path == null) {
            throw new IllegalArgumentException(ZOOKEEPER_PATH+" must be specified");
        }
        try {
            nextRolloverDate=System.currentTimeMillis()+tokenValidity;
            client.create().creatingParentsIfNeeded()
                    .forPath(path,generateZKData(generateRandomSecret(),generateRandomSecret(),null));
            zkVersion=0;
            LOG.info("Creating secret znode");
        }catch (KeeperException.NodeExistsException e){
            LOG.info("The secret znode already exists,retrieving data");
        }
        pullFromZK(true);
        long initialDelay=nextRolloverDate-System.currentTimeMillis();
        if (initialDelay < 1l) {
            int i=1;
            while (initialDelay < 1l) {
                initialDelay=nextRolloverDate+tokenValidity*i-System.currentTimeMillis();
                i++;
            }
        }
        super.startScheduler(initialDelay,tokenValidity);
    }

    private synchronized void pullFromZK(boolean isInit) {
        try {
            Stat stat = new Stat();
            byte[] bytes = client.getData().storingStatIn(stat).forPath(path);
            ByteBuffer bb = ByteBuffer.wrap(bytes);
            int dataVersion = bb.getInt();
            if (dataVersion > DATA_VERSION) {
                throw new IllegalArgumentException("Cannot load data from ZooKeeper; it was written with a newer version");
            }
            int nextSecretLength = bb.getInt();
            byte[] nextSecret = new byte[nextSecretLength];
            bb.get(nextSecret);
            this.nextSecret = nextSecret;
            zkVersion = stat.getVersion();
            if (isInit) {
                int currentSecretLength = bb.getInt();
                byte[] currentSecret = new byte[currentSecretLength];
                bb.get(currentSecret);
                int previouseSecretLength = bb.getInt();
                byte[] previousSecret = null;
                if (previouseSecretLength > 0) {
                    previousSecret = new byte[previouseSecretLength];
                    bb.get(previousSecret);
                }
                super.initSecrets(currentSecret, previousSecret);
                nextRolloverDate = bb.getLong();
            }
        }catch (Exception e){
            LOG.error("An unexpected exception occurred while pulling data from Zookeeper",e);
        }
    }

    private synchronized byte[] generateZKData(byte[] newSecret, byte[] currentSecret, byte[] previousSecret) {
        int newSecretLength = newSecret.length;
        int currentSecretLength = currentSecret.length;
        int previousSecretLength=0;
        if (previousSecret != null) {
            previousSecretLength=previousSecret.length;
        }
        ByteBuffer bb = ByteBuffer.allocate(INT_BYTES + INT_BYTES + newSecretLength + INT_BYTES + currentSecretLength + INT_BYTES + previousSecretLength + LONG_BYTES);
        bb.putInt(DATA_VERSION);
        bb.putInt(newSecretLength);
        bb.put(newSecret);
        bb.putInt(currentSecretLength);
        bb.put(currentSecret);
        bb.putInt(previousSecretLength);
        if (previousSecretLength > 0) {
            bb.put(previousSecret);
        }
        bb.putLong(nextRolloverDate);
        return bb.array();
    }

    private byte[] generateRandomSecret() {
        byte[] secret=new byte[32];
        random.nextBytes(secret);
        return secret;
    }

    protected CuratorFramework createCuratorClient(Properties config) {
        String connectingString = config.getProperty(ZOOKEEPER_CONNECTION_STRING, "localhost:2181");
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        ACLProvider aclProvider;
        String authType = config.getProperty(ZOOKEEPER_AUTH_TYPE, "none");
        if (authType.equals("sasl")) {
            LOG.info("Connecting to Zookeeper with SASL/Kerberos and using 'sasl' ACLs");
            String principal=setJaasConfiguration(config);
        }
        return null;
    }

    private String setJaasConfiguration(Properties config) {
        String keytabFile = config.getProperty(ZOOKEEPER_KERBEROS_KEYTAB).trim();
        if (keytabFile == null || keytabFile.length() == 0) {
            throw new IllegalArgumentException(ZOOKEEPER_KERBEROS_KEYTAB+" must be specified");
        }
        String principal = config.getProperty(ZOOKEEPER_KERBEROS_PRINCIPAL).trim();
        if (principal == null || principal.length() == 0) {
            throw new IllegalArgumentException(ZOOKEEPER_KERBEROS_PRINCIPAL+" must be specifed");
        }
        JaasConfiguration jConf = new JaasConfiguration(JAAS_LOGIN_ENTRY_NAME, principal, keytabFile);
        Configuration.setConfiguration(jConf);
        return principal.split("[/@]")[0];
    }

    @Override
    protected byte[] generateNewSecret() {
        return nextSecret;
    }
}
