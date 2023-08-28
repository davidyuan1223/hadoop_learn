package org.apache.hadoop.crypto.key;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class KeyProviderExtension<E extends KeyProviderExtension.Extension> extends KeyProvider {
    private KeyProvider keyProvider;
    private E extension;
    public KeyProviderExtension(KeyProvider keyProvider, E extension) {
        super(keyProvider.getConf());
        this.keyProvider = keyProvider;
        this.extension = extension;
    }
    protected E getExtension(){
        return extension;
    }

    public KeyProvider getKeyProvider() {
        return keyProvider;
    }

    @Override
    public boolean isTransient() {
        return keyProvider.isTransient();
    }

    @Override
    public KeyVersion getKeyVersion(String versionName) throws IOException {
        return keyProvider.getKeyVersion(versionName);
    }

    @Override
    public List<String> getKeys() throws IOException {
        return keyProvider.getKeys();
    }

    @Override
    public Metadata[] getKeysMetadata(String... names) throws IOException {
        return keyProvider.getKeysMetadata(names);
    }

    @Override
    public List<KeyVersion> getKeyVersions(String name) throws IOException {
        return keyProvider.getKeyVersions(name);
    }

    @Override
    public KeyVersion getCurrentKey(String name) throws IOException {
        return keyProvider.getCurrentKey(name);
    }

    @Override
    public Metadata getMetadata(String name) throws IOException {
        return keyProvider.getMetadata(name);
    }

    @Override
    public KeyVersion createKey(String name, byte[] material, Options options) throws IOException {
        return keyProvider.createKey(name,material,options);
    }

    @Override
    public KeyVersion createKey(String name, Options options) throws NoSuchAlgorithmException, IOException {
        return keyProvider.createKey(name,options);
    }

    @Override
    public void deleteKey(String name) throws IOException {
        keyProvider.deleteKey(name);
    }

    @Override
    public KeyVersion rollNewVersion(String name, byte[] material) throws IOException {
        return keyProvider.rollNewVersion(name,material);
    }

    @Override
    public void flush() throws IOException {
        keyProvider.flush();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ": " + keyProvider.toString();
    }
    public static interface Extension{

    }
}
