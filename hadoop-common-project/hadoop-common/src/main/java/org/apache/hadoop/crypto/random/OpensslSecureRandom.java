package org.apache.hadoop.crypto.random;

import com.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.PerformanceAdvisory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.Random;
@InterfaceAudience.Private
public class OpensslSecureRandom extends Random {
    private static final long serialVersionUID=-7828193502768789584L;
    private static final Logger LOG= LoggerFactory.getLogger(OpensslSecureRandom.class.getName());
    private SecureRandom fallback=null;
    private static boolean nativeEnabled=false;
    static {
        if (NativeCodeLoader.isNativeCodeLoaded()
                && NativeCodeLoader.buildSupportsOpenssl()) {
            try {
                initSR();
                nativeEnabled=true;
            }catch (Throwable t){
                LOG.error("Failed to load Openssl SecureRandom",t);
            }
        }
    }
    public static boolean isNativeCodeLoaded(){
        return nativeEnabled;
    }
    public OpensslSecureRandom(){
        if (!nativeEnabled) {
            PerformanceAdvisory.LOG.debug("Build does not support openssl, " +
                    "falling back to Java SecureRandom");
            fallback=new SecureRandom();
        }
    }

    @Override
    public void nextBytes(byte[] bytes) {
        if (!nativeEnabled || !nextRandBytes(bytes)) {
            fallback.nextBytes(bytes);
        }
    }

    @Override
    public synchronized void setSeed(long seed) {

    }
    private native static void initSR();
    private native boolean nextRandBytes(byte[] bytes);

    @Override
    final protected int next(int bits) {
        Preconditions.checkArgument(bits>=0 && bits<=32);
        int numBytes=(bits+7)/8;
        byte[] b = new byte[bits];
        int next=0;
        nextBytes(b);
        for (int i = 0; i < numBytes; i++) {
            next=(next<<8)+(b[i]&0xFF);
        }
        return next>>>(numBytes*8-bits);
    }
}
