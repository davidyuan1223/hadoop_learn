package org.apache.hadoop.crypto;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.thirdparty.com.google.common.base.Splitter;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.util.PerformanceAdvisory;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.security.GeneralSecurityException;
import java.util.List;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class CryptoCodec implements Configurable, Closeable {
    public static Logger LOG= LoggerFactory.getLogger(CryptoCodec.class);
    public static CryptoCodec getInstance(Configuration conf,CipherSuite cipherSuite){
        List<Class<? extends CryptoCodec>> klasses=getCodecClasses(conf,cipherSuite);
        if (klasses == null) {
            return null;
        }
        CryptoCodec codec=null;
        for (Class<? extends CryptoCodec> klass : klasses) {
            try {
                CryptoCodec c= ReflectionUtils.newInstance(klass,conf);
                if (c.getCipherSuite().getName().equals(cipherSuite.getName())) {
                    if (codec == null) {
                        PerformanceAdvisory.LOG.debug("Using crypto codec {}.",klass.getName());
                        codec=c;
                    }
                }else {
                    PerformanceAdvisory.LOG.debug("Crypto codec {} doesn't meet the cipher suite {}.",
                            klass.getName(),cipherSuite.getName());
                }
            }catch (Exception e){
                PerformanceAdvisory.LOG.debug("Crypto codec {} is not available.",klass.getName());
            }
        }
        return codec;
    }
    public static CryptoCodec getInstance(Configuration conf){
        String name = conf.get(CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_KEY,
                CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_DEFAULT);
        return getInstance(conf,CipherSuite.convert(name));
    }
    private static List<Class<? extends CryptoCodec>> getCodecClasses(Configuration conf,CipherSuite cipherSuite){
        List<Class<? extends CryptoCodec>> result= Lists.newArrayList();
        String configName = CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_KEY_PREFIX + cipherSuite.getConfigSuffix();
        String codecString;
        if (configName.equals(CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_AES_CTR_NOPADDING_KEY)) {
            codecString=conf.get(configName,CommonConfigurationKeysPublic
                    .HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_AES_CTR_NOPADDING_DEFAULT);
        } else if (configName.equals(CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_SM4_CTR_NOPADDING_KEY)) {
            codecString=conf.get(configName,CommonConfigurationKeysPublic
                    .HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_SM4_CTR_NOPADDING_DEFAULT);
        }else {
            codecString=conf.get(configName);
        }
        if (codecString == null) {
            PerformanceAdvisory.LOG.debug(
                    "No crypto codec classes with cipher suite configured."
            );
            return null;
        }
        for (String c : Splitter.on(',').trimResults().omitEmptyStrings().split(codecString)) {
            try {
                Class<?> cls = conf.getClassByName(c);
                result.add(cls.asSubclass(CryptoCodec.class));
            }catch (ClassCastException e){
                PerformanceAdvisory.LOG.debug("Class {} is not a CryptoCodec",c);
            }catch (ClassNotFoundException e){
                PerformanceAdvisory.LOG.debug("Crypto codec {} not found",c);
            }
        }
        return result;
    }
    public abstract CipherSuite getCipherSuite();
    public abstract Encryptor createEncryptor()throws GeneralSecurityException;
    public abstract Decryptor createDecryptor()throws GeneralSecurityException;
    public abstract void calculateIV(byte[] initIV,long counter,byte[] IV);
    public abstract void generateSecureRandom(byte[] bytes);
}
