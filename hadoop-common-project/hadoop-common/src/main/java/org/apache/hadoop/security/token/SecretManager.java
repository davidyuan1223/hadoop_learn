package org.apache.hadoop.security.token;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ipc.StandbyException;

import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class SecretManager<T extends TokenIdentifier> {
    @SuppressWarnings("serial")
    public static class InvalidToken extends IOException{
        public InvalidToken(String msg){super(msg);}
    }
    protected abstract byte[] createPassword(T identifier);
    public abstract byte[] retrievePassword(T identifier)throws InvalidToken;
    public byte[] retriableRetrievePassword(T identifier)throws InvalidToken, StandbyException,IOException{
        return retrievePassword(identifier);
    }
    public abstract T createIdentifier();
    public void checkAvailableForRead()throws StandbyException{}
    private static final String DEFAULT_HMAC_ALGORITHM="HmacSHA1";
    private static final int KEY_LENGTH=64;
    private static final ThreadLocal<Mac> threadLocalMac=new ThreadLocal<Mac>(){
        @Override
        protected Mac initialValue() {
            try {
                return Mac.getInstance(DEFAULT_HMAC_ALGORITHM);
            }catch (NoSuchAlgorithmException e){
                throw new IllegalArgumentException("Can not find "+DEFAULT_HMAC_ALGORITHM+" algorithm.");
            }
        }
    };
    private final KeyGenerator kenGen;
    {
        try {
            kenGen=KeyGenerator.getInstance(DEFAULT_HMAC_ALGORITHM);
            kenGen.init(KEY_LENGTH);
        }catch (NoSuchAlgorithmException e){
            throw new IllegalArgumentException("Can not find "+DEFAULT_HMAC_ALGORITHM+" algorithm.");
        }
    }
    protected SecretKey generateSecret(){
        SecretKey key;
        synchronized (kenGen){
            key=kenGen.generateKey();
        }
        return key;
    }
    public static byte[] createPassword(byte[] identifier,SecretKey key){
        Mac mac = threadLocalMac.get();
        try {
            mac.init(key);
        }catch (InvalidKeyException e){
            throw new IllegalArgumentException("Invalid key to HMAC computation",e);
        }
        return mac.doFinal(identifier);
    }
    protected static SecretKey createSecretKey(byte[] key){
        return new SecretKeySpec(key,DEFAULT_HMAC_ALGORITHM);
    }
}
