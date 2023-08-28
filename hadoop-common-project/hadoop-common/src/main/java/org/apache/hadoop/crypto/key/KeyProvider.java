package org.apache.hadoop.crypto.key;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import javax.crypto.KeyGenerator;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.util.*;

@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class KeyProvider implements Closeable {
    public static final String DEFAULT_CIPHER_NAME=
            CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_DEFAULT_CIPHER_KEY;
    public static final String DEFAULT_CIPHER=
            CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_DEFAULT_CIPHER_DEFAULT;
    public static final String DEFAULT_BITLENGTH_NAME=
            CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_DEFAULT_BITLENGTH_KEY;
    public static final int DEFAULT_BITLENGTH=
            CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_DEFAULT_BITLENGTH_DEFAULT;
    public static final String JCEKS_KEY_SERIALFILTER_DEFAULT=
            "java.lang.Enum;" +
                    "java.security.KeyRep;" +
                    "java.security.KeyRep$Type;" +
                    "javax.crypto.spec.SecretKeySpec;" +
                    "org.apache.hadoop.crypto.key.JavaKeyStoreProvider$KeyMetadata;" +
                    "!*";
    public static final String JCEKS_KEY_SERIAL_FILTER="jceks.key.serialFilter";
    private final Configuration conf;
    public KeyProvider(Configuration conf){
        this.conf=new Configuration(conf);
        if (System.getProperty(JCEKS_KEY_SERIAL_FILTER) == null) {
            String serialFilter = conf.get(CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCEKS_KEY_SERIALFILTER,
                    JCEKS_KEY_SERIALFILTER_DEFAULT);
            System.setProperty(JCEKS_KEY_SERIAL_FILTER,serialFilter);
        }
        String jceProvider = conf.get(CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY);
        if (BouncyCastleProvider.PROVIDER_NAME.equals(jceProvider)) {
            Security.addProvider(new BouncyCastleProvider());
        }
    }

    public Configuration getConf() {
        return conf;
    }
    public static Options options(Configuration conf){
        return new Options(conf);
    }
    public boolean isTransient(){
        return false;
    }
    public abstract KeyVersion getKeyVersion(String versionName)throws IOException;
    public abstract List<String > getKeys()throws IOException;
    public Metadata[] getKeysMetadata(String... names)throws IOException{
        Metadata[] result = new Metadata[names.length];
        for (int i = 0; i < names.length; i++) {
            result[i]=getMetadata(names[i]);
        }
        return result;
    }
    public abstract List<KeyVersion> getKeyVersions(String name)throws IOException;
    public KeyVersion getCurrentKey(String name)throws IOException{
        Metadata meta=getMetadata(name);
        if (meta == null) {
            return null;
        }
        return getKeyVersion(buildVersionName(name,meta.getVersions()-1));
    }
    public abstract Metadata getMetadata(String name)throws IOException;
    public abstract KeyVersion createKey(String name,byte[] material,Options options)throws IOException;
    private String getAlgorithm(String cipher){
        int slash = cipher.indexOf('/');
        if (slash==-1) {
            return cipher;
        }else {
            return cipher.substring(0,slash);
        }
    }
    protected byte[] generateKey(int size,String algorithm)throws NoSuchAlgorithmException{
        algorithm=getAlgorithm(algorithm);
        KeyGenerator generator=KeyGenerator.getInstance(algorithm);
        generator.init(size);
        return generator.generateKey().getEncoded();
    }
    public KeyVersion createKey(String name,Options options)throws NoSuchAlgorithmException,IOException{
        byte[] material = generateKey(options.getBitLength(), options.getCipher());
        return createKey(name,material,options);
    }
    public abstract void deleteKey(String name)throws IOException;
    public abstract KeyVersion rollNewVersion(String name,byte[] material)throws IOException;
    public void close()throws IOException{

    }
    public KeyVersion rollNewVersion(String name)throws NoSuchAlgorithmException,IOException{
        Metadata meta = getMetadata(name);
        if (meta == null) {
            throw new IOException("Can't find Metadata for key "+name);
        }
        byte[] material = generateKey(meta.getBitLength(), meta.getCipher());
        return rollNewVersion(name,material);
    }
    public void invalidateCache(String name)throws IOException{

    }
    public abstract void flush()throws IOException;
    public static String getBaseName(String versionName)throws IOException{
        Objects.requireNonNull(versionName,"VersionName cannot be null");
        int div = versionName.lastIndexOf('@');
        if (div==-1) {
            throw new IOException("No version in key path "+versionName);
        }
        return versionName.substring(0,div);
    }
    protected static String buildVersionName(String name,int version){
        return name+"@"+version;
    }
    public static KeyProvider findProvider(List<KeyProvider> providers,
                                           String keyName)throws IOException{
        for (KeyProvider provider : providers) {
            if (provider.getMetadata(keyName) != null) {
                return provider;
            }
        }
        throw new IOException("Can't find KeyProvider for key "+keyName);
    }
    public boolean needsPassword()throws IOException{
        return false;
    }
    public String noPasswordWarning(){
        return null;
    }
    public String noPasswordError(){
        return null;
    }

    public static class KeyVersion{
        private final String name;
        private final String versionName;
        private final byte[] material;

        protected KeyVersion(String name,String versionName,byte[] material){
            this.name=name==null?null:name.intern();
            this.versionName=versionName==null?null:versionName.intern();
            this.material=material;
        }

        public String getName() {
            return name;
        }

        public String getVersionName() {
            return versionName;
        }

        public byte[] getMaterial() {
            return material;
        }
        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder();
            buf.append("key(");
            buf.append(versionName);
            buf.append(")=");
            if (material == null) {
                buf.append("null");
            } else {
                for(byte b: material) {
                    buf.append(' ');
                    int right = b & 0xff;
                    if (right < 0x10) {
                        buf.append('0');
                    }
                    buf.append(Integer.toHexString(right));
                }
            }
            return buf.toString();
        }

        @Override
        public boolean equals(Object rhs) {
            if (this == rhs) {
                return true;
            }
            if (rhs == null || getClass() != rhs.getClass()) {
                return false;
            }
            final KeyVersion kv = (KeyVersion) rhs;
            return Objects.equals(name, kv.name)
                    && Objects.equals(versionName, kv.versionName)
                    && Arrays.equals(material, kv.material);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, versionName, Arrays.hashCode(material));
        }
    }

    public static class Metadata{
        private static final String CIPHER_FIELD="cipher";
        private static final String BIT_LENGTH_FIELD="bigLength";
        private static final String CREATED_FIELD="created";
        private static final String DESCRIPTION_FIELD="description";
        private static final String VERSIONS_FIELD="versions";
        private static final String ATTRIBUTES_FIELD="attributes";
        private final String cipher;
        private final int bitLength;
        private final Date created;
        private final String description;
        private int versions;
        private Map<String ,String > attributes;

        protected Metadata(String cipher,int bitLength,String description,Map<String,String > attributes,Date created,int versions){
            this.cipher=cipher;
            this.bitLength=bitLength;
            this.description=description;
            this.attributes=(attributes==null||attributes.isEmpty())?null:attributes;
            this.created=created;
            this.versions=versions;
        }
        public String toString() {
            final StringBuilder metaSB = new StringBuilder();
            metaSB.append("cipher: ").append(cipher).append(", ");
            metaSB.append("length: ").append(bitLength).append(", ");
            metaSB.append("description: ").append(description).append(", ");
            metaSB.append("created: ").append(created).append(", ");
            metaSB.append("version: ").append(versions).append(", ");
            metaSB.append("attributes: ");
            if ((attributes != null) && !attributes.isEmpty()) {
                for (Map.Entry<String, String> attribute : attributes.entrySet()) {
                    metaSB.append("[");
                    metaSB.append(attribute.getKey());
                    metaSB.append("=");
                    metaSB.append(attribute.getValue());
                    metaSB.append("], ");
                }
                metaSB.deleteCharAt(metaSB.length() - 2);  // remove last ', '
            } else {
                metaSB.append("null");
            }
            return metaSB.toString();
        }

        public String getDescription() {
            return description;
        }

        public Date getCreated() {
            return created;
        }

        public String getCipher() {
            return cipher;
        }

        public Map<String, String> getAttributes() {
            return attributes== null?Collections.emptyMap():attributes;
        }

        public String getAlgorithm(){
            int slash = cipher.indexOf('/');
            if (slash==-1) {
                return cipher;
            }else {
                return cipher.substring(0,slash);
            }
        }

        public int getBitLength() {
            return bitLength;
        }

        public int getVersions() {
            return versions;
        }
        protected int addVersion(){
            return versions++;
        }
        protected byte[] serialize()throws IOException{
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            JsonWriter writer = new JsonWriter(new OutputStreamWriter(buffer, StandardCharsets.UTF_8));
            try {
                writer.beginObject();
                if (cipher != null) {
                    writer.name(CIPHER_FIELD)
                            .value(cipher);
                }
                if (bitLength!=0) {
                    writer.name(BIT_LENGTH_FIELD)
                          .value(bitLength);
                }
                if (description!=null) {
                    writer.name(DESCRIPTION_FIELD)
                          .value(description);
                }
                if (created!=null) {
                    writer.name(CREATED_FIELD)
                         .value(created.getTime());
                }
                if (attributes != null && attributes.size() > 0) {
                    writer.name(ATTRIBUTES_FIELD).beginObject();
                    for (Map.Entry<String, String> attribute : attributes.entrySet()) {
                        writer.name(attribute.getKey())
                                .value(attribute.getValue());
                    }
                    writer.endObject();
                }
                writer.name(VERSIONS_FIELD)
                        .value(versions);
                writer.endObject();
                writer.flush();
            }finally {
                writer.close();
            }
            return buffer.toByteArray();
        }
        protected Metadata(byte[] bytes)throws IOException{
            String cipher=null;
            int bitLength=0;
            Date created=null;
            String description=null;
            int versions=0;
            Map<String,String> attributes=null;
            JsonReader reader = new JsonReader(new InputStreamReader(new ByteArrayInputStream(bytes), StandardCharsets.UTF_8));
            try {
                reader.beginObject();
                while (reader.hasNext()) {
                    String field = reader.nextName();
                    if (CIPHER_FIELD.equals(field)) {
                        cipher=reader.nextString();
                    } else if (BIT_LENGTH_FIELD.equals(field)) {
                        bitLength=reader.nextInt();
                    }else if (CREATED_FIELD.equals(field)) {
                        created = new Date(reader.nextLong());
                    } else if (VERSIONS_FIELD.equals(field)) {
                        versions = reader.nextInt();
                    } else if (DESCRIPTION_FIELD.equals(field)) {
                        description = reader.nextString();
                    } else if (ATTRIBUTES_FIELD.equalsIgnoreCase(field)) {
                        reader.beginObject();
                        attributes = new HashMap<String, String>();
                        while (reader.hasNext()) {
                            attributes.put(reader.nextName(), reader.nextString());
                        }
                        reader.endObject();
                    }
                }
                reader.endObject();
            }finally {
                reader.close();
            }
            this.cipher = cipher;
            this.bitLength = bitLength;
            this.created = created;
            this.description = description;
            this.attributes = attributes;
            this.versions = versions;
        }
    }
    public static class Options{
        private String cipher;
        private int bitLength;
        private String description;
        private Map<String,String > attributes;

        public Options(Configuration conf){
            cipher=conf.get(DEFAULT_CIPHER_NAME,DEFAULT_CIPHER);
            bitLength=conf.getInt(DEFAULT_BITLENGTH_NAME,DEFAULT_BITLENGTH);
        }
        public Options setCipher(String cipher){
            this.cipher=cipher;
            return this;
        }
        public Options setBitLength(int bitLength){
            this.bitLength=bitLength;
            return this;
        }
        public Options setDescription(String description){
            this.description=description;
            return this;
        }
        public Options setAttributes(Map<String,String > attributes){
            if (attributes != null) {
                if (attributes.containsKey(null)) {
                    throw new IllegalArgumentException("attributes cannot have NULL key");
                }
                this.attributes=new HashMap<>(attributes);
            }
            return this;
        }

        public String getCipher() {
            return cipher;
        }

        public int getBitLength() {
            return bitLength;
        }

        public String getDescription() {
            return description;
        }

        public Map<String, String> getAttributes() {
            return attributes==null?Collections.emptyMap():attributes;
        }
        @Override
        public String toString() {
            return "Options{" +
                    "cipher='" + cipher + '\'' +
                    ", bitLength=" + bitLength +
                    ", description='" + description + '\'' +
                    ", attributes=" + attributes +
                    '}';
        }
    }
}
