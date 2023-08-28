package org.apache.hadoop.crypto.key;

import com.apache.hadoop.classification.InterfaceAudience;
import com.sun.tools.javah.Gen;
import org.apache.hadoop.crypto.CryptoCodec;
import org.apache.hadoop.crypto.Decryptor;
import org.apache.hadoop.crypto.Encryptor;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.List;
import java.util.ListIterator;

@InterfaceAudience.Private
public class KeyProviderCryptoExtension extends KeyProviderExtension<KeyProviderCryptoExtension.CryptoExtension>{
    public static final String EEK="EEK";
    public static final String EK="EK";

    protected KeyProviderCryptoExtension(KeyProvider keyProvider,CryptoExtension extension){
        super(keyProvider,extension);
    }
    public void warmUpEncryptedKeys(String ... keyNames)throws IOException{
        getExtension().warmUpEncryptedKeys(keyNames);
    }
    public EncryptedKeyVersion generateEncryptedKey(String encryptedKeyName)throws IOException, GeneralSecurityException{
        return getExtension().generateEncryptedKey(encryptedKeyName);
    }
    public KeyVersion decryptEncryptedKey(EncryptedKeyVersion encryptedKey)
            throws IOException, GeneralSecurityException {
        return getExtension().decryptEncrptedKey(encryptedKey);
    }
    public EncryptedKeyVersion reencryptEncryptedKey(EncryptedKeyVersion ekv)
            throws IOException, GeneralSecurityException {
        return getExtension().reencryptedEncryptedKey(ekv);
    }
    public void drain(String keyName) {
        getExtension().drain(keyName);
    }
    public void reencryptEncryptedKeys(List<EncryptedKeyVersion> ekvs)
            throws IOException, GeneralSecurityException {
        getExtension().reencryptEncrptedKeys(ekvs);
    }
    public static KeyProviderCryptoExtension createKeyProviderCryptoExtension(KeyProvider keyProvider){
        CryptoExtension cryptoExtension=null;
        if (keyProvider instanceof CryptoExtension){
            cryptoExtension=(CryptoExtension)keyProvider;
        }else if (keyProvider instanceof KeyProviderExtension
        &&((KeyProviderExtension)keyProvider).getKeyProvider() instanceof KeyProviderCryptoExtension.CryptoExtension){
            KeyProviderExtension keyProviderExtension =
                    (KeyProviderExtension)keyProvider;
            cryptoExtension =
                    (CryptoExtension)keyProviderExtension.getKeyProvider();
        }else {
            cryptoExtension = new DefaultCryptoExtension(keyProvider);
        }
        return new KeyProviderCryptoExtension(keyProvider, cryptoExtension);
    }
    @Override
    public void close() throws IOException {
        KeyProvider provider = getKeyProvider();
        if (provider != null && provider != this) {
            provider.close();
        }
    }



    public interface CryptoExtension extends KeyProviderExtension.Extension{
        public void warmUpEncryptedKeys(String ... keyNames)throws IOException;
        public void drain(String keyName);
        public EncryptedKeyVersion generateEncryptedKey(String encryptionKeyName)throws IOException, GeneralSecurityException;
        public KeyVersion decryptEncrptedKey(EncryptedKeyVersion encryptedKeyVersion)throws IOException,GeneralSecurityException;
        EncryptedKeyVersion reencryptedEncryptedKey(EncryptedKeyVersion ekv)throws IOException,GeneralSecurityException;
        void reencryptEncrptedKeys(List<EncryptedKeyVersion> ekvs)throws IOException,GeneralSecurityException;
    }
    public static class EncryptedKeyVersion{
        private String encryptionKeyName;
        private String encryptionKeyVersionName;
        private byte[] encryptedKeyIv;
        private KeyVersion encryptedKeyVersion;
        protected EncryptedKeyVersion(String keyname,String encryptionKeyVersionName,byte[] encryptedKeyIv,
                                      KeyVersion encryptedKeyVersion){
            this.encryptionKeyName=keyname==null?null:keyname.intern();
            this.encryptionKeyVersionName=encryptionKeyVersionName==null?null:encryptionKeyVersionName.intern();
            this.encryptedKeyIv=encryptedKeyIv;
            this.encryptedKeyVersion=encryptedKeyVersion;
        }
        public static EncryptedKeyVersion createForDecryption(String keyName,String encryptionKeyVersionName,byte[] encryptedKeyIv,
                                                              byte[] encryptedKeyMaterial){
            KeyVersion encryptedKeyVersion = new KeyVersion(null, EEK, encryptedKeyMaterial);
            return new EncryptedKeyVersion(keyName,encryptionKeyVersionName,encryptedKeyIv,encryptedKeyVersion);
        }

        public String getEncryptionKeyName() {
            return encryptionKeyName;
        }

        public String getEncryptionKeyVersionName() {
            return encryptionKeyVersionName;
        }

        public byte[] getEncryptedKeyIv() {
            return encryptedKeyIv;
        }

        public KeyVersion getEncryptedKeyVersion() {
            return encryptedKeyVersion;
        }
        protected static byte[] deriveIV(byte[] encryptedKeyIv){
            byte[] rIv = new byte[encryptedKeyIv.length];
            for (int i = 0; i < encryptedKeyIv.length; i++) {
                rIv[i]=(byte) (encryptedKeyIv[i]^0xff);
            }
            return rIv;
        }
    }
    private static class DefaultCryptoExtension implements CryptoExtension{
        private final KeyProvider keyProvider;
        private static final ThreadLocal<SecureRandom> RANDOM=new ThreadLocal<SecureRandom>(){
            @Override
            protected SecureRandom initialValue() {
                return new SecureRandom();
            }
        };
        private DefaultCryptoExtension(KeyProvider keyProvider){
            this.keyProvider=keyProvider;
        }

        @Override
        public EncryptedKeyVersion generateEncryptedKey(String encryptionKeyName) throws IOException, GeneralSecurityException {
            KeyVersion encryptionKey = keyProvider.getCurrentKey(encryptionKeyName);
            Preconditions.checkNotNull(encryptionKey,"No KeyVersion exists for key '%s' ",encryptionKeyName);
            CryptoCodec cc=CryptoCodec.getInstance(keyProvider.getConf());
            try {
                final byte[] newKey=new byte[encryptionKey.getMaterial().length];
                cc.generateSecureRandom(newKey);
                final byte[] iv=new byte[cc.getCipherSuite().getAlgorithmBlockSize()];
                cc.generateSecureRandom(iv);
                Encryptor encryptor=cc.createEncryptor();
                return generateEncryptedKey(encryptor,encryptionKey,newKey,iv);
            }finally {
                cc.close();
            }
        }
        private EncryptedKeyVersion generateEncryptedKey(final Encryptor encryptor,
                                                         final KeyVersion encryptionKey,
                                                         final byte[] key,
                                                         final byte[] iv) throws IOException, GeneralSecurityException {
            final byte[] encryptionIV=EncryptedKeyVersion.deriveIV(iv);
            encryptor.init(encryptionKey.getMaterial(),encryptionIV);
            final int keyLen=key.length;
            ByteBuffer bbIn = ByteBuffer.allocateDirect(keyLen);
            ByteBuffer bbOut = ByteBuffer.allocateDirect(keyLen);
            bbIn.put(key);
            bbIn.flip();
            encryptor.encrypt(bbIn,bbOut);
            bbOut.flip();
            byte[] encrptedKey = new byte[keyLen];
            bbOut.get(encrptedKey);
            return new EncryptedKeyVersion(encryptionKey.getName(),
                    encryptionKey.getVersionName(),iv,new KeyVersion(encryptionKey.getName(),EEK,encrptedKey));
        }

        @Override
        public EncryptedKeyVersion reencryptedEncryptedKey(EncryptedKeyVersion ekv) throws IOException, GeneralSecurityException {
            final String ekName=ekv.getEncryptionKeyName();
            final KeyVersion ekNow=keyProvider.getCurrentKey(ekName);
            Preconditions.checkNotNull(ekNow,"KeyVersion name '%s' does not exist");
            Preconditions.checkArgument(ekv.getEncryptedKeyVersion().getVersionName().equals(KeyProviderCryptoExtension.EEK),
                    "encryptedKey version name must be '%s', but found '%s'",
                    KeyProviderCryptoExtension.EEK,
                    ekv.getEncryptedKeyVersion().getVersionName());
            if (ekv.getEncryptedKeyVersion().equals(ekNow)) {
                return ekv;
            }
            final KeyVersion dek=decryptEncrptedKey(ekv);
            final CryptoCodec cc=CryptoCodec.getInstance(keyProvider.getConf());
            try {
                final Encryptor encryptor=cc.createEncryptor();
                return generateEncryptedKey(encryptor,ekNow,dek.getMaterial(),ekv.getEncryptedKeyIv());
            }finally {
                cc.close();
            }
        }

        @Override
        public void reencryptEncrptedKeys(List<EncryptedKeyVersion> ekvs) throws IOException, GeneralSecurityException {
            Preconditions.checkNotNull(ekvs,"Input list is null");
            KeyVersion ekNow=null;
            Decryptor decryptor=null;
            Encryptor encryptor=null;
            try(CryptoCodec cc=CryptoCodec.getInstance(keyProvider.getConf())){
                decryptor=cc.createDecryptor();
                encryptor=cc.createEncryptor();
                ListIterator<EncryptedKeyVersion> iter = ekvs.listIterator();
                while (iter.hasNext()) {
                    final EncryptedKeyVersion ekv=iter.next();
                    Preconditions.checkNotNull(ekv,"EncryptedKeyVersion is null");
                    final String ekName = ekv.getEncryptionKeyName();
                    Preconditions.checkNotNull(ekName,"Key name is null");
                    Preconditions.checkNotNull(ekv.getEncryptedKeyVersion(),"EncryptedKeyVersion is null");
                    Preconditions.checkArgument(ekv.getEncryptedKeyVersion().getVersionName().equals(KeyProviderCryptoExtension.EEK),
                            "encryptedKey version name must be '%s',but found '%s'",
                            KeyProviderCryptoExtension.EEK,
                            ekv.getEncryptedKeyVersion().getVersionName());
                    if (ekNow == null) {
                        ekNow=keyProvider.getCurrentKey(ekName);
                        Preconditions.checkNotNull(ekNow,"Key name '%s' does not exist");
                    }else {
                        Preconditions.checkArgument(ekNow.getName().equals(ekName),
                                "All keys must have the same key name. Expected '%s'" +
                                        "but found '%s'",ekNow.getName(),ekName);
                    }
                    final String encryptionKeyVersionName=ekv.getEncryptionKeyVersionName();
                    final KeyVersion encryptionKey=keyProvider.getKeyVersion(encryptionKeyVersionName);
                    Preconditions.checkNotNull(encryptionKey,"KeyVersion name '%s' does not exist",encryptionKeyVersionName);
                    if (encryptionKey.equals(ekNow)) {
                        continue;
                    }
                    final KeyVersion ek=decryptEncryptedKey(decryptor,encryptionKey,ekv);
                    iter.set(generateEncryptedKey(encryptor,ekNow,ek.getMaterial(),ekv.getEncryptedKeyIv()));
                }
            }
        }

        private KeyVersion decryptEncryptedKey(final Decryptor decryptor,final KeyVersion encryptionKey,final EncryptedKeyVersion encryptedKeyVersion) throws IOException, GeneralSecurityException {
            final byte[] encryptionIV=EncryptedKeyVersion.deriveIV(encryptedKeyVersion.getEncryptedKeyIv());
            decryptor.init(encryptionKey.getMaterial(),encryptionIV);
            final KeyVersion encryptedKV=encryptedKeyVersion.getEncryptedKeyVersion();
            int keyLen=encryptedKV.getMaterial().length;
            ByteBuffer bbIn = ByteBuffer.allocateDirect(keyLen);
            ByteBuffer bbOut = ByteBuffer.allocateDirect(keyLen);
            bbIn.put(encryptedKV.getMaterial());
            bbIn.flip();
            decryptor.decrypt(bbIn,bbOut);
            bbOut.flip();
            byte[] decryptedKey = new byte[keyLen];
            bbOut.get(decryptedKey);
            return new KeyVersion(encryptionKey.getName(),EK,decryptedKey);
        }

        @Override
        public KeyVersion decryptEncrptedKey(EncryptedKeyVersion encryptedKeyVersion) throws IOException, GeneralSecurityException {
            final String encryptionKeyVersionName=encryptedKeyVersion.getEncryptionKeyVersionName();
            final KeyVersion encryptionKey=keyProvider.getKeyVersion(encryptionKeyVersionName);
            Preconditions.checkNotNull(encryptionKey,"KeyVersion name '%s' does not exist",encryptionKeyVersionName);
            Preconditions.checkArgument(encryptedKeyVersion.getEncryptedKeyVersion().getVersionName().equals(KeyProviderCryptoExtension.EEK),
                    "encryptedKey version name must be '%s', but found '%s'",
                    KeyProviderCryptoExtension.EEK,
                    encryptedKeyVersion.getEncryptedKeyVersion().getVersionName());
            try(CryptoCodec cc=CryptoCodec.getInstance(keyProvider.getConf())){
                final Decryptor decryptor = cc.createDecryptor();
                return decryptEncryptedKey(decryptor, encryptionKey,
                        encryptedKeyVersion);
            }
        }

        @Override
        public void warmUpEncryptedKeys(String... keyNames) throws IOException {

        }

        @Override
        public void drain(String keyName) {

        }
    }
}
