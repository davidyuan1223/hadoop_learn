package org.apache.hadoop.crypto.key;

import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hadoop.thirdparty.com.google.common.cache.LoadingCache;
import org.apache.hadoop.util.bloom.Key;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class CachingKeyProvider extends KeyProviderExtension<CachingKeyProvider.CacheExtension>{
    @Override
    public KeyVersion getCurrentKey(String name)throws IOException{
        try {
            return getExtension().currentKeyCache.get(name);
        }catch (ExecutionException e){
            Throwable cause = e.getCause();
            if (cause instanceof KeyNotFoundException) {
                return null;
            } else if (cause instanceof IOException) {
                throw (IOException) cause;
            }else {
                throw new IOException(cause);
            }
        }
    }
    @Override
    public KeyVersion getKeyVersion(String versionName)throws IOException{
        try {
            return getExtension().keyVersionCache.get(versionName);
        }catch (ExecutionException e){
            Throwable cause = e.getCause();
            if (cause instanceof KeyNotFoundException) {
                return null;
            } else if (cause instanceof IOException) {
                throw (IOException) cause;
            }else {
                throw new IOException(cause);
            }
        }
    }

    public CachingKeyProvider(KeyProvider keyProvider,long keyTimeoutMillis,long currKeyTimeoutMillis){
        super(keyProvider,new CacheExtension(keyProvider,keyTimeoutMillis,currKeyTimeoutMillis));
    }
    @Override
    public void deleteKey(String name)throws IOException{
        getKeyProvider().deleteKey(name);
        getExtension().currentKeyCache.invalidate(name);
        getExtension().keyMetadataCache.invalidate(name);
        getExtension().keyVersionCache.invalidateAll();
    }
    @Override
    public KeyVersion rollNewVersion(String name,byte[] material)throws IOException{
        KeyVersion key=getKeyProvider().rollNewVersion(name,material);
        invalidateCache(name);
        return key;
    }
    @Override
    public KeyVersion rollNewVersion(String name)throws NoSuchAlgorithmException,IOException{
        KeyVersion key=getKeyProvider().rollNewVersion(name);
        invalidateCache(name);
        return key;
    }
    @Override
    public void invalidateCache(String name)throws IOException{
        getKeyProvider().invalidateCache(name);
        getExtension().currentKeyCache.invalidate(name);
        getExtension().keyMetadataCache.invalidate(name);
        getExtension().keyVersionCache.invalidateAll();
    }

    @Override
    public Metadata getMetadata(String name)throws IOException{
        try {
            return getExtension().keyMetadataCache.get(name);
        }catch (ExecutionException e){
            Throwable cause = e.getCause();
            if (cause instanceof KeyNotFoundException) {
                return null;
            } else if (cause instanceof IOException) {
                throw (IOException) cause;
            }else {
                throw new IOException(cause);
            }
        }
    }




    static class CacheExtension implements KeyProviderExtension.Extension{
        private final KeyProvider provider;
        private LoadingCache<String ,KeyVersion> keyVersionCache;
        private LoadingCache<String ,KeyVersion> currentKeyCache;
        private LoadingCache<String ,Metadata> keyMetadataCache;

        CacheExtension(KeyProvider prov,long keyTimeoutMillis,long currKeyTimeoutMillis){
            this.provider=prov;
            keyVersionCache=CacheBuilder.newBuilder()
                    .expireAfterAccess(keyTimeoutMillis,TimeUnit.MILLISECONDS)
                    .build(new CacheLoader<String, KeyVersion>() {
                        @Override
                        public KeyVersion load(String key) throws Exception {
                            KeyVersion kv=provider.getKeyVersion(key);
                            if (kv == null) {
                                throw new KeyNotFoundException();
                            }
                            return kv;
                        }
                    });
            keyMetadataCache=CacheBuilder.newBuilder()
                    .expireAfterAccess(keyTimeoutMillis,TimeUnit.MILLISECONDS)
                    .build(new CacheLoader<String, Metadata>() {
                        @Override
                        public Metadata load(String key) throws Exception {
                            Metadata meta=provider.getMetadata(key);
                            if (meta == null) {
                                throw new KeyNotFoundException();
                            }
                            return meta;
                        }
                    });
            currentKeyCache=CacheBuilder.newBuilder().expireAfterAccess(keyTimeoutMillis,TimeUnit.MILLISECONDS)
                    .build(new CacheLoader<String, KeyVersion>() {
                        @Override
                        public KeyVersion load(String key) throws Exception {
                            KeyVersion kv=provider.getCurrentKey(key);
                            if (kv == null) {
                                throw new KeyNotFoundException();
                            }
                            return kv;
                        }
                    });
        }
    }

    @SuppressWarnings("serial")
    private static class KeyNotFoundException extends Exception{}
}
