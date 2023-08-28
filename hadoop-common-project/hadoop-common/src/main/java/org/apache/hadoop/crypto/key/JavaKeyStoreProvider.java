package org.apache.hadoop.crypto.key;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.net.URI;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@InterfaceAudience.Private
public class JavaKeyStoreProvider extends KeyProvider{
    private static final String KEY_METADATA="KeyMetadata";
    private static final Logger LOG= LoggerFactory.getLogger(JavaKeyStoreProvider.class);
    public static final String SCHEME_NAME="jecks";
    public static final String KEYSTORE_PASSWORD_FILE_KEY=
            "hadoop.security.keystore.java-keystore-provider.password.file";
    public static final String KEYSTORE_PASSWORD_ENV_VAR=
            "HADOOP_KEYSTORE_PASSWORD";
    public static final char[] KEYSTORE_PASSWORD_DEFAULT="none".toCharArray();
    private final URI uri;
    private final Path path;
    private final FileSystem fs;
    private FsPermission permissions;
    private KeyStore keyStore;
    private char[] password;
    private boolean changed=false;
    private Lock readLock;
    private Lock writeLock;
    private final Map<String ,Metadata> cache=new HashMap<>();
    @VisibleForTesting
    JavaKeyStoreProvider(JavaKeyStoreProvider other){
        super(new Configuration());
        uri=other.uri;
        path=other.path;
        fs=other.fs;
        permissions=other.permissions;
        keyStore=other.keyStore;
        password=other.password;
        changed=other.changed;
        readLock=other.readLock;
        writeLock=other.writeLock;
    }
    private JavaKeyStoreProvider(URI uri,Configuration conf)throws IOException{
        super(conf);
        this.uri=uri;
        path= ProviderUtils.unnestUri(uri);
        fs=path.getFileSystem(conf);
        locateKeystore();
        ReadWriteLock lock=new ReentrantReadWriteLock(true);
        readLock=lock.readLock();
        writeLock=lock.writeLock();
    }
    private void locateKeystore() throws IOException{
        try {
            password=ProviderUtils.locatedPassword(KEYSTORE_PASSWORD_ENV_VAR,
                    getConf().get(KEYSTORE_PASSWORD_FILE_KEY));
            if(password==null){
                password=KEYSTORE_PASSWORD_DEFAULT;
            }
            Path oldPath=constructOldPath(path);
            Path newPath=constructNewPath(path);
            keyStore=KeyStore.getInstance(SCHEME_NAME);
            FsPermission perm=null;
            if (fs.exists(path)) {
                if (fs.exists(newPath)) {
                    throw new IOException(String.format("Keystore not loaded due to some inconsistency " +
                            "('%s' and '%s' should not exist together)!!",path,newPath));
                }
                perm=tryLoadFromPath(path,oldPath);
            }else {
                perm=tryLoadIncompleteFlush(oldPath,newPath);
            }
            permissions=perm;
        }catch (KeyStoreException e){
            throw new IOException("Can't create keystore: "+e,e);
        }catch (GeneralSecurityException e){
            throw new IOException("Can't load keystore "+path+" : "+e,e);
        }
    }
    private FsPermission tryLoadFromPath(Path path,Path backupPath) throws KeyStoreException,
            GeneralSecurityException, IOException{
        FsPermission perm=null;
        try {
            perm=loadFromPath(path,password);
            fs.delete(backupPath,true);
            LOG.debug("KeyStore loaded successfully !!");
        }catch (IOException e){
            if (!isBadorWrongPassword(e)) {
                perm=loadFromPath(backupPath,password);
                renameOrFail(path,new Path(path.toString()+"_CORRUPTED_"+System.currentTimeMillis()));
                renameOrFail(backupPath,path);
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format(
                            "Keystore loaded successfully from '%s' since '%s' was cprrupted !!",backupPath,path
                    ));
                }
            }else {
                throw e;
            }
        }
        return perm;
    }

    private FsPermission tryLoadIncompleteFlush(Path oldPath,Path newPath) throws KeyStoreException,
            GeneralSecurityException, IOException{
        FsPermission perm=null;
        if (fs.exist(newPath)) {
            perm=loadAndReturnPerm(newPath,oldPath);
        }
        if (perm == null && fs.exist(oldPath)) {
            perm=loadAndReturnPerm(oldPath,newPath);
        }
        if (perm == null) {
            keyStore.load(null,password);
            LOG.debug("KeyStore initialized a new successfully !!");
            perm=new FsPermission("600");
        }
        return perm;
    }

    private FsPermission loadAndReturnPerm(Path pathToLoad, Path pathToDelete) throws IOException, CertificateException, NoSuchAlgorithmException {
        FsPermission perm=null;
        try {
            perm=loadFromPath(pathToLoad,password);
            renameOrFail(pathToLoad,path);
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("KeyStore loaded successfully from '%s' !!",pathToLoad));
            }
            fs.delete(pathToDelete,true);
        }catch (IOException e){
            if (isBadorWrongPassword(e)) {
                throw e;
            }
        }
        return perm;
    }


    private boolean isBadorWrongPassword(IOException e) {
        if (e.getCause() instanceof UnrecoverableKeyException) {
            return true;
        }
        if ((e.getCause()==null)
        &&(e.getMessage()!=null)
        &&((e.getMessage().contains("Keystore was tampered"))
        ||(e.getMessage().contains("password was incorrect")))){
            return true;
        }
        return false;
    }

    private FsPermission loadFromPath(Path path, char[] password) throws CertificateException, IOException, NoSuchAlgorithmException {
        try(FSDataInputStream in=fs.open(path)){
            FileStatus status=fs.getFileStatus(path);
            keyStore.load(in,password);
            return status.getPermission();
        }
    }

    private static Path constructNewPath(Path path){
        return new Path(path.toString()+"_NEW");
    }
    private static Path constructOldPath(Path path){
        return new Path(path.toString()+"_OLD");
    }
    @Override
    public boolean needsPassword()throws IOException{
        return (null==ProviderUtils.locatedPassword(KEYSTORE_PASSWORD_ENV_VAR,
                getConf().get(KEYSTORE_PASSWORD_FILE_KEY)));
    }
    @Override
    public String noPasswordWarning(){
        return ProviderUtils.noPasswordWarning(KEYSTORE_PASSWORD_ENV_VAR,KEYSTORE_PASSWORD_FILE_KEY);
    }
    @Override
    public String noPasswordError(){
        return ProviderUtils.noPasswordError(KEYSTORE_PASSWORD_ENV_VAR,KEYSTORE_PASSWORD_FILE_KEY);
    }
    @Override
    public KeyVersion getKeyVersion(String versionName)throws IOException{
        readLock.lock();
        try {
            SecretKeySpec key=null;
            try {
                if (!keyStore.containsAlias(versionName)) {
                    return null;
                }
                key=(SecretKeySpec) keyStore.getKey(versionName,password);
            }catch (KeyStoreException e){
                throw new IOException("Can't get key "+versionName+" from "+path,e);
            }catch (NoSuchAlgorithmException e){
                throw new IOException("Can't get algorithm for key "+key+" from "+path,e);
            }catch (UnrecoverableKeyException e){
                throw new IOException("Can't get recover key "+key+" from "+path,e);
            }
            return new KeyVersion(getBaseName(versionName),versionName,key.getEncoded());
        }finally {
            readLock.unlock();
        }
    }
    @Override
    public List<String > getKeys() throws IOException{
        readLock.lock();
        try {
            ArrayList<String > list=new ArrayList<>();
            String alias=null;
            try {
                Enumeration<String> e = keyStore.aliases();
                while (e.hasMoreElements()) {
                    alias=e.nextElement();
                    if (!alias.contains("@")) {
                        list.add(alias);
                    }
                }
            }catch (KeyStoreException e){
                throw new IOException("Can't get key "+alias+" from "+path,e);
            }
            return list;
        }finally {
            readLock.unlock();
        }
    }
    @Override
    public List<KeyVersion> getKeyVersions(String name)throws IOException{
        readLock.lock();
        try {
            List<KeyVersion> list=new ArrayList<>();
            Metadata km=getMetadata(name);
            if (km != null) {
                int latestVersion=km.getVersions();
                KeyVersion v=null;
                String versionName=null;
                for (int i = 0; i < latestVersion; i++) {
                    versionName=buildVersionName(name,i);
                    v=getKeyVersion(versionName);
                    if (v != null) {
                        list.add(v);
                    }
                }
            }
            return list;
        }finally {
            readLock.unlock();
        }
    }

    @Override
    public Metadata getMetadata(String name) throws IOException {
        readLock.lock();
        try {
            if (cache.containsKey(name)) {
                return cache.get(name);
            }
            try {
                if (!keyStore.containsAlias(name)) {
                    return null;
                }
                Metadata meta=((KeyMetadata)keyStore.getKey(name,password)).metadata;
                cache.put(name,meta);
                return meta;
            }catch (ClassCastException e){
                throw new IOException("Can not cast key for "+name+" in keystore "+
                        path+" to a KeyMetadata. Key may have been added using keytool " +
                        "or some other non-Hadoop method.",e);
            }catch (KeyStoreException e){
                throw new IOException("Can not get metadata for "+name+" from keystore "+path,e);
            }catch (NoSuchAlgorithmException e){
                throw new IOException("Can not get algorithm for "+name+" from keystore "+path,e);
            }catch (UnrecoverableKeyException e){
                throw new IOException("Can not recover key for "+name+" from keystore "+path,e);
            }
        }finally {
            readLock.unlock();
        }
    }

    @Override
    public KeyVersion createKey(String name, byte[] material, Options options) throws IOException {
        Preconditions.checkArgument(name.equals(StringUtils.toLowerCase(name)),
                "Uppercase key names are unsupported: %s",name);
        writeLock.lock();
        try {
            try {
                if (keyStore.containsAlias(name) || cache.containsKey(name)) {
                    throw new IOException("Key "+name+" already exists in "+this);
                }
            }catch (KeyStoreException e){
                throw new IOException("Problem looking up key "+name+" in "+this,e);
            }
            Metadata metadata = new Metadata(options.getCipher(), options.getBitLength(), options.getDescription(), options.getAttributes(),
                    new Date(), 1);
            if (options.getBitLength() != 8 * material.length) {
                throw new IOException("Wrong key length. Required "+options.getBitLength()+", but got "
                +(8*material.length));
            }
            cache.put(name,metadata);
            String versionName = buildVersionName(name, 0);
            return innerSetKeyVersion(name,versionName,material,metadata.getCipher());
        }finally {
            readLock.unlock();
        }
    }

    @Override
    public void deleteKey(String name) throws IOException {
        writeLock.lock();
        try {
            Metadata meta = getMetadata(name);
            if (meta == null) {
                throw new IOException("Key "+name+" does not exist in "+this);
            }
            for (int i = 0; i < meta.getVersions(); i++) {
                String versionName = buildVersionName(name, i);
                try {
                    if (keyStore.containsAlias(versionName)) {
                        keyStore.deleteEntry(versionName);
                    }
                }catch (KeyStoreException e){
                    throw new IOException("Problem removing "+versionName+" from "+this,e);
                }
            }
            try {
                if (keyStore.containsAlias(name)) {
                    keyStore.deleteEntry(name);
                }
            }catch (KeyStoreException e){
                throw new IOException("Problem removing "+name+" from "+this,e);
            }
            cache.remove(name);
            changed=true;
        }finally {
            writeLock.unlock();
        }
    }
    KeyVersion innerSetKeyVersion(String name,String versionName,byte[] material,
                                  String cipher) throws IOException {
        try {
            keyStore.setKeyEntry(versionName, new SecretKeySpec(material, cipher), password, null);
        } catch (KeyStoreException e) {
            throw new IOException("Can't store key " + versionName + " in " + this, e);
        }
        changed=true;
        return new KeyVersion(name,versionName,material);
    }

    @Override
    public KeyVersion rollNewVersion(String name, byte[] material) throws IOException {
        writeLock.lock();
        try {
            Metadata meta = getMetadata(name);
            if (meta == null) {
                throw new IOException("Key "+name+" does not exist in "+this);
            }
            if (meta.getBitLength() != 8 * material.length) {
                throw new IOException("Wrong key length. Required "+meta.getBitLength()+
                        ",but got "+(8*material.length));
            }
            int nextVersion = meta.addVersion();
            String versionName = buildVersionName(name, nextVersion);
            return innerSetKeyVersion(name,versionName,material,meta.getCipher());
        }finally {
            writeLock.unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        Path newPath = constructNewPath(path);
        Path oldPath = constructOldPath(path);
        Path resetPath=path;
        writeLock.lock();
        try {
            if (!changed) {
                return;
            }
            try {
                renameOrFail(newPath,new Path(newPath.toString()+"_ORPHANED_"+System.currentTimeMillis()));
            }catch (FileNotFoundException e){

            }
            try {
                renameOrFail(oldPath,new Path(oldPath.toString()+"_ORPHANED_"+System.currentTimeMillis()));
            }catch (FileNotFoundException e){

            }
            for (Map.Entry<String, Metadata> entry : cache.entrySet()) {
                try {
                    keyStore.setKeyEntry(entry.getKey(),new KeyMetadata(entry.getValue()),password,null);
                }catch (KeyStoreException e){
                    throw new IOException("Can't set metadata key "+entry.getKey(),e);
                }
            }
            boolean fileExisted=backupToOld(oldPath);
            if (fileExisted) {
                resetPath=oldPath;
            }
            try {
                writeToNew(newPath);
            }catch (IOException e){
                revertFromOld(oldPath,fileExisted);
                resetPath=path;
                throw e;
            }
            cleanupNewAndOld(newPath,oldPath);
            changed=false;
        }catch (IOException e){
            resetKeyStoreState(resetPath);
            throw e;
        }finally {
            writeLock.unlock();
        }
    }
    private void resetKeyStoreState(Path path){
        LOG.debug("Could not flush keystore..." +
                "attempting to reset to previous state !!");
        cache.clear();
        try {
            loadFromPath(path,password);
            LOG.debug("keystore resetting to previously flushed state !!");
        }catch (Exception e){
            LOG.debug("Could not reset KeyStore to previous state",e);
        }
    }
    private void cleanupNewAndOld(Path newPath,Path oldPath)throws IOException{
        renameOrFail(newPath,path);
        fs.delete(oldPath,true);
    }
    protected void writeToNew(Path newPath)throws IOException{
        try(FSDataOutputStream out=FileSystem.create(fs,newPath,permissions);){
            keyStore.store(out,password);
        }catch (KeyStoreException e){
            throw new IOException("Can't store keystore "+this,e);
        }catch (NoSuchAlgorithmException e){
            throw new IOException("No such algorithm storing keystore "+this,e);
        }catch (CertificateException e){
            throw new IOException("Certificate exception storing keystore "+this,e);
        }
    }
    protected boolean backupToOld(Path oldPath)throws IOException{
        try {
            renameOrFail(path,oldPath);
            return true;
        }catch (FileNotFoundException e){
            return false;
        }
    }
    protected void revertFromOld(Path oldPath,boolean fileExisted)throws IOException{
        if (fileExisted) {
            renameOrFail(oldPath,path);
        }
    }
    private void renameOrFail(Path src,Path dest)throws IOException{
        if (!fs.rename(src, dest)) {
            throw new IOException("Rename unsuccessful : "+String.format("'%s' to '%s'",src,dest));
        }
    }
    @Override
    public String toString() {
        return uri.toString();
    }

    public static class Factory extends KeyProviderFactory{

        @Override
        public KeyProvider createProvider(URI providerName, Configuration conf) throws IOException {
            if (SCHEME_NAME.equals(providerName.getScheme())) {
                return new JavaKeyStoreProvider(providerName,conf);
            }
            return null;
        }
    }

    public static class KeyMetadata implements Key, Serializable{
        private Metadata metadata;
        private static final long serialVersionUID=8405872419967874451L;
        private KeyMetadata(Metadata metadata){
            this.metadata=metadata;
        }

        @Override
        public String getAlgorithm() {
            return metadata.getCipher();
        }

        @Override
        public String getFormat() {
            return KEY_METADATA;
        }

        @Override
        public byte[] getEncoded() {
            return new byte[0];
        }
        private void writeObject(ObjectOutputStream out)throws IOException{
            byte[] serialize = metadata.serialize();
            out.writeInt(serialize.length);
            out.write(serialize);
        }
        private void readObject(ObjectInputStream in)throws IOException, ClassNotFoundException{
            byte[] buf = new byte[in.readInt()];
            in.readFully(buf);
            metadata=new Metadata(buf);
        }
    }


}
