package org.apache.hadoop.crypto.key;

import com.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.URI;
import java.util.*;

@InterfaceAudience.Private
public class UserProvider extends KeyProvider{
    public static final String SCHEME_NAME="user";
    private final UserGroupInformation user;
    private final Credentials credentials;
    private final Map<String ,Metadata> cache=new HashMap<>();

    private UserProvider(Configuration conf)throws IOException{
        super(conf);
        user=UserGroupInformation.getCurrentUser();
        credentials=user.getCredentials();
    }

    @Override
    public boolean isTransient() {
        return true;
    }

    @Override
    public KeyVersion getKeyVersion(String versionName) throws IOException {
        byte[] bytes=credentials.getSecretKey(new Text(versionName));
        if (bytes == null) {
            return null;
        }
        return new KeyVersion(getBaseName(versionName),versionName,bytes);
    }

    @Override
    public Metadata getMetadata(String name) throws IOException {
        if (cache.containsKey(name)) {
            return cache.get(name);
        }
        byte[] serialized=credentials.getSecretKey(new Text(name));
        if (serialized == null) {
            return null;
        }
        Metadata result = new Metadata(serialized);
        cache.put(name,result);
        return result;
    }

    @Override
    public KeyVersion createKey(String name, byte[] material, Options options) throws IOException {
        Text nameT = new Text(name);
        if (credentials.getSecretKey(nameT) != null) {
            throw new IOException("Key "+name+" already exists in "+this);
        }
        if (options.getBitLength() != 8 * material.length) {
            throw new IOException("Wrong key length. Required "
            +options.getBitLength()+",but got "+(8*material.length));
        }
        Metadata meta = new Metadata(options.getCipher(), options.getBitLength(),
                options.getDescription(), options.getAttributes(), new Date(), 1);
        cache.put(name,meta);
        String versionName = buildVersionName(name, 0);
        credentials.addSecretKey(nameT,meta.serialize());
        credentials.addSecretKey(new Text(versionName),material);
        return new KeyVersion(name,versionName,material);
    }

    @Override
    public void deleteKey(String name) throws IOException {
        Metadata meta = getMetadata(name);
        if (meta == null) {
            throw new IOException("Key "+name+" does not exist in "+this);
        }
        for (int i = 0; i < meta.getVersions(); i++) {
            credentials.removeSecretKey(new Text(buildVersionName(name,i)));
        }
        credentials.removeSecretKey(new Text(name));
        cache.remove(name);
    }

    @Override
    public synchronized KeyVersion rollNewVersion(String name, byte[] material) throws IOException {
        Metadata meta = getMetadata(name);
        if (meta == null) {
            throw new IOException("Key "+name+" not found");
        }
        if (meta.getBitLength() != 8 * material.length) {
            throw new IOException("Wrong key length. Required "+meta.getBitLength()
            +", but got "+(8*material.length));
        }
        int nextVersion = meta.addVersion();
        credentials.addSecretKey(new Text(name),meta.serialize());
        String versionName = buildVersionName(name, nextVersion);
        credentials.addSecretKey(new Text(versionName),material);
        return new KeyVersion(name,versionName,material);
    }

    @Override
    public String toString() {
        return SCHEME_NAME+":///";
    }

    @Override
    public synchronized void flush() throws IOException {
        user.addCredentials(credentials);
    }
    public static class Factory extends KeyProviderFactory{
        @Override
        public KeyProvider createProvider(URI providerName, Configuration conf) throws IOException {
            if (SCHEME_NAME.equals(providerName.getScheme())) {
                return new UserProvider(conf);
            }
            return null;
        }
    }

    @Override
    public synchronized List<String> getKeys() throws IOException {
        List<String > list=new ArrayList<>();
        List<Text> keys=credentials.getAllSecretKeys();
        for (Text key : keys) {
            if (key.find("@") == -1) {
                list.add(key.toString());
            }
        }
        return list;
    }

    @Override
    public synchronized List<KeyVersion> getKeyVersions(String name) throws IOException {
        List<KeyVersion> list=new ArrayList<>();
        Metadata meta = getMetadata(name);
        if (meta != null) {
            int latestVersion = meta.getVersions();
            for (int i = 0; i < latestVersion; i++) {
                KeyVersion v = getKeyVersion(buildVersionName(name, i));
                if (v != null) {
                    list.add(v);
                }
            }
        }
        return list;
    }
}
