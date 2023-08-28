package org.apache.hadoop.crypto.random;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;
@InterfaceAudience.Private
public class OsSecureRandom extends Random implements Closeable, Configurable {
    public static final Logger LOG= LoggerFactory.getLogger(OpensslSecureRandom.class);
    private static final long serialVersionUID=6391500337172057900L;
    private transient Configuration conf;
    private final int RESERVOIR_LENGTH=8192;
    private String randomDevPath;
    private transient InputStream stream;
    private final byte[] reservoir=new byte[RESERVOIR_LENGTH];
    private int pos=reservoir.length;

    private void fillReservoir(int min){
        if (pos >= reservoir.length - min) {
            try {
                if (stream == null) {
                    stream= Files.newInputStream(Paths.get(randomDevPath));
                }
                IOUtils.readFully(stream,reservoir,0,reservoir.length);
            }catch (IOException e){
                throw new RuntimeException("failed to fill reservoir",e);
            }
            pos=0;
        }
    }
    public OsSecureRandom(){}
    @VisibleForTesting
    public boolean isClosed(){
        return stream==null;
    }

    @Override
    public synchronized void setConf(Configuration conf) {
        this.conf = conf;
        this.randomDevPath=conf.get(
                CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_KEY,
                CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_DEFAULT
        );
        close();
    }

    @Override
    public synchronized Configuration getConf() {
        return conf;
    }

    @Override
    public void nextBytes(byte[] bytes) {
        int off=0;
        int n=0;
        while (off < bytes.length) {
            fillReservoir(0);
            n=Math.min(bytes.length-off,reservoir.length-pos);
            System.arraycopy(reservoir,pos,bytes,off,n);
            off+=n;
            pos+=n;
        }
    }

    @Override
    synchronized protected int next(int nbits){
        fillReservoir(4);
        int n=0;
        for (int i = 0; i < 4; i++) {
            n=(n<<8) | (reservoir[pos++] & 0xff);
        }
        return n & (0xffffffff >> (32-nbits));
    }

    @Override
    synchronized public void close() {
        if (stream != null) {
            IOUtils.cleanupWithLogger(LOG,stream);
            stream=null;
        }
    }

    @Override
    protected void finalize() throws Throwable {
        close();
    }
}
