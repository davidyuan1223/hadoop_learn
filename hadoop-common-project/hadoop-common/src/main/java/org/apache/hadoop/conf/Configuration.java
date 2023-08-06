package org.apache.hadoop.conf;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import com.apache.hadoop.classification.VisibleForTesting;
import com.ctc.wstx.api.ReaderConfig;
import com.ctc.wstx.io.StreamBootstrapper;
import com.ctc.wstx.io.SystemId;
import com.ctc.wstx.stax.WstxInputFactory;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.commons.collections.map.UnmodifiableMap;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.XMLUtils;
import org.apache.http.util.NetUtils;
import org.codehaus.stax2.XMLStreamReader2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.annotation.Nullable;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.lang.ref.WeakReference;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class Configuration implements Iterable<Map.Entry<String ,String >>, Writable {
    private static final Logger LOG= LoggerFactory.getLogger(Configuration.class);
    private static final Logger LOG_DEPRECATION=LoggerFactory.getLogger("org.apache.hadoop.conf.Configuration.deprecation");
    private boolean allowNullValueProperties=false;
    private static final Map<ClassLoader,Map<String ,WeakReference<Class<?>>>> CACHE_CLASSED
            =new WeakHashMap<>();
    private ClassLoader classLoader;
    private static final String DEFAULT_STRING_CHECK="testingforemptydefaultvalue";
    private static DeprecationDelta[] defaultDeprecations=
            new DeprecationDelta[]{
                    new DeprecationDelta("topology.script.file.name",
                            CommonConfigurationKeys.NET_DEPENDENCY_SCRIPT_FILE_NAME_KEY),
                    new DeprecationDelta("topology.script.number.args",
                            CommonConfigurationKeys.NET_TOPOLOGY_SCRIPT_NUMBER_ARGS_KEY),
                    new DeprecationDelta("hadoop.configured.node.mapping",
                            CommonConfigurationKeys.NET_TOPOLOGY_CONFIGURED_NODE_MAPPING_KEY),
                    new DeprecationDelta("topology.node.switch.mapping.impl",
                            CommonConfigurationKeys.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY),
                    new DeprecationDelta("dfs.df.interval",
                            CommonConfigurationKeys.FS_DF_INTERVAL_KEY),
                    new DeprecationDelta("fs.default.name",
                            CommonConfigurationKeys.FS_DEFAULT_NAME_KEY),
                    new DeprecationDelta("dfs.umaskmode",
                            CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY),
                    new DeprecationDelta("dfs.nfs.exports.allowed.hosts",
                            CommonConfigurationKeys.NFS_EXPORTS_ALLOWED_HOSTS_KEY)
            };
    private static final CopyOnWriteArrayList<String > defaultResources=new CopyOnWriteArrayList<>();
    private static AtomicReference<DeprecationContext> deprecationContext=new AtomicReference<>(new DeprecationContext(null,defaultDeprecations));
    private Set<String > finalParameters=Collections.newSetFromMap(new ConcurrentHashMap<>());
    private boolean loadDefaults=true;
    private static final int MAX_SUBST=20;
    private static final Class<?> NEGATIVE_CACHE_SENTINEL=NegativeCacheSentinel.class;
    private Properties overlay;
    private Properties properties;
    private final Map<String ,Properties> propertyTagsMap=new ConcurrentHashMap<>();
    private boolean quietmode=true;
    private static final WeakHashMap<Configuration,Object> REGISTRY=new WeakHashMap<>();
    private ArrayList<Resource> resources=new ArrayList<>();
    private boolean restrictSystemProps=restrictSystemPropsDefault;
    private static boolean restrictSystemPropsDefault=false;
    private static final  int SUB_START_IDX=0;
    private static final int SUB_END_IDX=SUB_START_IDX+1;
    private static final Set<String > TAGS=ConcurrentHashMap.newKeySet();
    static final String UNKNOWN_RESOURCE="Unknown";
    private volatile Map<String ,String[]> updatingResources;
    private static final WstxInputFactory XML_INPUT_FACTORY=new WstxInputFactory();

    {
        classLoader=Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader=Configuration.class.getClassLoader();
        }
    }
    static {
        addDefaultResource("core-default.xml");
        addDefaultResource("core-site.xml");
        ClassLoader cl=Thread.currentThread().getContextClassLoader();
        if (cl == null) {
            cl=Configuration.class.getClassLoader();
        }
        if (cl.getResource("hadoop-site.xml") != null) {
            LOG.warn("DEPRECATED: hadoop-site.xml found in the classpath. Usage of hadoop-site.xml is deprecated." +
                    "Instead use core-site.xml,mapred-site-xml and hdfs-site.xml to override properties of core-default.xml" +
                    ",mapred-default.xml and hdfs-default.xml respectively");
            addDefaultResource("hadoop-site.xml");
        }
    }

    public Configuration(){this(true);}
    public Configuration(boolean loadDefaults){
        this.loadDefaults=loadDefaults;
        synchronized (Configuration.class){
            REGISTRY.put(this,null);
        }
    }
    @SuppressWarnings("unchecked")
    public Configuration(Configuration other){
        synchronized (other){
            other.getProps();
            this.resources= (ArrayList<Resource>) other.resources.clone();
            if (other.properties != null) {
                this.properties=(Properties) other.properties.clone();
            }
            if (other.overlay != null) {
                this.overlay= (Properties) other.overlay.clone();
            }
            this.restrictSystemProps=other.restrictSystemProps;
            if (other.updatingResources != null) {
                this.updatingResources=new ConcurrentHashMap<>(other.updatingResources);
            }
            this.finalParameters=Collections.newSetFromMap(new ConcurrentHashMap<>());
            this.finalParameters.addAll(other.finalParameters);
            this.propertyTagsMap.putAll(other.propertyTagsMap);
        }
        synchronized (Configuration.class){
            REGISTRY.put(this,null);
        }
        this.classLoader=other.classLoader;
        this.loadDefaults=other.loadDefaults;
        setQuietMode(other.getQuietMode());
    }
    public static synchronized void addDefaultResource(String name){
        if (!defaultResources.contains(name)) {
            defaultResources.add(name);
            for (Configuration conf : REGISTRY.keySet()) {
                if (conf.loadDefaults) {
                    conf.reloadConfiguration();
                }
            }
        }
    }
    public static void addDeprecation(String key,String newKey){
        addDeprecation(key,new String[] {newKey},null);
    }
    public static void addDeprecation(String key,String newKey,String customMessage){
        addDeprecation(key,new String[]{newKey},customMessage);
    }
    @Deprecated
    public static void addDeprecation(String key,String[] newKey){
        addDeprecation(key,newKey,null);
    }
    @Deprecated
    public static void addDeprecation(String key,String[] newKey,String customMessage){
        addDeprecation(new DeprecationDelta[]{new DeprecationDelta(key,newKey,customMessage)});
    }
    public static void addDeprecation(DeprecationDelta[] deltas){
        DeprecationContext prev,next;
        do{
            prev=deprecationContext.get();
            next=new DeprecationContext(prev,deltas);
        }while (!deprecationContext.compareAndSet(prev,next));
    }
    public void addResource(Configuration conf){
        addResourceObject(new Resource(conf.getProps(),conf.restrictSystemProps));
    }
    public void addResource(InputStream in){
        addResourceObject(new Resource(in));
    }
    public void addResource(InputStream in,boolean restrictedParser){
        addResourceObject(new Resource(in,restrictedParser));
    }
    public void addResource(InputStream in,String name){
        addResourceObject(new Resource(in,name));
    }
    public void addResource(InputStream in,String name,boolean restrictedParser){
        addResourceObject(new Resource(in,name,restrictedParser));
    }
    public void addResource(Path file){
        addResourceObject(new Resource(file));
    }
    public void addResource(Path file,boolean restrictParser){
        addResourceObject(new Resource(file,restrictParser));
    }
    public void addResource(String name){
        addResourceObject(new Resource(name));
    }
    public void addResource(String name,boolean restrictedParser){
        addResourceObject(new Resource(name,restrictedParser));
    }
    public void addResource(URL url){
        addResourceObject(new Resource(url));
    }
    public void addResource(URL url,boolean restrictedParser){
        addResourceObject(new Resource(url,restrictedParser));
    }
    public void addTags(Properties prop){
        try {
            if (prop.containsKey(CommonConfigurationKeys.HADOOP_TAGS_SYSTEM)) {
                String systemTags = prop.getProperty(CommonConfigurationKeys.HADOOP_TAGS_SYSTEM);
                TAGS.addAll(Arrays.asList(systemTags.split(",")));
            }
            if (prop.containsKey(CommonConfigurationKeys.HADOOP_TAGS_CUSTOM)) {
                String customTags = prop.getProperty(CommonConfigurationKeys.HADOOP_TAGS_CUSTOM);
                TAGS.addAll(Arrays.asList(customTags.split(",")));
            }
            if (prop.containsKey(CommonConfigurationKeys.HADOOP_SYSTEM_TAGS)) {
                String systemTags = prop.getProperty(CommonConfigurationKeys.HADOOP_SYSTEM_TAGS);
                TAGS.addAll(Arrays.asList(systemTags.split(",")));
            }
            if (prop.containsKey(CommonConfigurationKeys.HADOOP_CUSTOM_TAGS)) {
                String customTags = prop.getProperty(CommonConfigurationKeys.HADOOP_CUSTOM_TAGS);
                TAGS.addAll(Arrays.asList(customTags.split(",")));
            }
        }catch (Exception e){
            LOG.trace("Error adding tags in configuration",e);
        }
    }
    private static void appendJSONProperty(JsonGenerator jsonGen,
                                           Configuration conf,String name,ConfigRedactor redactor)throws IOException{
        if (!Strings.isNullOrEmpty(name) && jsonGen != null) {
            jsonGen.writeStartObject();
            jsonGen.writeStringField("key",name);
            jsonGen.writeStringField("value",redactor.redact(name,conf.get(name)));
            jsonGen.writeBooleanField("isFinal",conf.finalParameters.contains(name));
            String[] resources=conf.updatingResources!=null?conf.updatingResources.get(name):null;
            String resource=UNKNOWN_RESOURCE;
            if (resources!=null && resources.length>0){
                resource=resources[0];
            }
            jsonGen.writeStringField("resource",resource);
            jsonGen.writeEndObject();
        }
    }
    private synchronized void appendXMLProperty(Document doc, Element conf,String propertyName,ConfigRedactor redactor){
        if (!Strings.isNullOrEmpty(propertyName)) {
            String value = properties.getProperty(propertyName);
            if (value != null) {
                Element propNode = doc.createElement("property");
                conf.appendChild(propNode);
                Element nameNode = doc.createElement("name");
                nameNode.appendChild(doc.createTextNode(propertyName));
                propNode.appendChild(nameNode);
                Element valueNode = doc.createElement("value");
                String propertyValue = properties.getProperty(propertyName);
                if (redactor != null) {
                    propertyValue=redactor.redactXml(propertyName,propertyValue);
                }
                valueNode.appendChild(doc.createTextNode(propertyValue));
                propNode.appendChild(valueNode);
                Element finalNode = doc.createElement("final");
                finalNode.appendChild(doc.createTextNode(String.valueOf(finalParameters.contains(propertyName))));
                propNode.appendChild(finalNode);
                if (updatingResources != null) {
                    String[] sources = updatingResources.get(propertyName);
                    if (sources != null) {
                        for (String source : sources) {
                            Element sourceNode = doc.createElement("source");
                            sourceNode.appendChild(doc.createTextNode(source));
                            propNode.appendChild(sourceNode);
                        }
                    }
                }
            }
        }
    }

    private synchronized void addResourceObject(Resource resource){
        resources.add(resource);
        restrictSystemProps|=resource.isRestrictParser();
        loadProps(properties,resources.size()-1,false);
    }

    private synchronized Document asXmlDocument(@Nullable String propertyName,ConfigRedactor redactor)throws IOException,IllegalArgumentException{
        Document doc;
        try {
            doc= DocumentBuilderFactory
                    .newInstance()
                    .newDocumentBuilder().newDocument();
        }catch (ParserConfigurationException e){
            throw new IOException(e);
        }
        Element conf = doc.createElement("configuration");
        doc.appendChild(conf);
        conf.appendChild(doc.createTextNode("\n"));
        handleDeprecation();
        if (!Strings.isNullOrEmpty(propertyName)) {
            if (!properties.containsKey(propertyName)) {
                throw new IllegalArgumentException("Property "+propertyName+" not found");
            }else {
                appendXMLProperty(doc,conf,propertyName,redactor);
                conf.appendChild(doc.createTextNode("\n"));
            }
        }else {

            for (Enumeration<Object> key = properties.keys();key.hasMoreElements();){
                appendXMLProperty(doc,conf,(String) key.nextElement(),redactor);
                conf.appendChild(doc.createTextNode("\n"));
            }
        }
        return doc;
    }
    private void checkForOverride(Properties properties,String name,String attr,String value){
        String propertyValue = properties.getProperty(attr);
        if (propertyValue != null && !propertyValue.equals(value)) {
            LOG.warn(name+":an attempt to override final parameter: "+attr+"; Ignoring.");
        }
    }
    public void clear(){
        getProps().clear();
        getOverlay().clear();
    }
    private double convertStorageUnit(double value,StorageUnit sourceUnit,StorageUnit targetUnit){
        double byteValue = sourceUnit.toBytes(value);
        return targetUnit.fromBytes(byteValue);
    }
    public static void dumpConfiguration(Configuration conf,String propertyName,Writer out)throws IOException{
        if (!Strings.isNullOrEmpty(propertyName)) {
            dumpConfiguration(conf,out);
        } else if (Strings.isNullOrEmpty(conf.get(propertyName))) {
            throw new IllegalArgumentException("Property "+propertyName+" not found");
        }else {
            JsonFactory dumpFactory = new JsonFactory();
            JsonGenerator dumpGenerator = dumpFactory.createGenerator(out);
            dumpGenerator.writeStartObject();
            dumpGenerator.writeFieldName("property");
            appendJSONProperty(dumpGenerator,conf,propertyName,new ConfigRedactor(conf));
            dumpGenerator.writeEndObject();
            dumpGenerator.flush();
        }
    }
    public static void dumpConfiguration(Configuration conf,Writer out)throws IOException{
        JsonFactory dumpFactory = new JsonFactory();
        JsonGenerator dumpGenerator = dumpFactory.createGenerator(out);
        dumpGenerator.writeStartObject();
        dumpGenerator.writeFieldName("properties");
        dumpGenerator.writeStartArray();
        dumpGenerator.flush();
        ConfigRedactor redactor = new ConfigRedactor(conf);
        synchronized (conf){
            for(Map.Entry<Object,Object> item: conf.getProps().entrySet()){
                appendJSONProperty(dumpGenerator,conf,item.getKey().toString(),redactor);
            }
        }
        dumpGenerator.writeEndArray();
        dumpGenerator.writeEndObject();
        dumpGenerator.flush();
    }
    public static void dumpDeprecatedKeys(){
        DeprecationContext deprecationContext = Configuration.deprecationContext.get();
        for (Map.Entry<String, DeprecatedKeyInfo> entry : deprecationContext.getDeprecatedKeyInfoMap().entrySet()) {
            StringBuilder newKeys = new StringBuilder();
            for (String newKey : entry.getValue().newKeys) {
                newKeys.append(newKey).append("\t");
            }
            System.out.println(entry.getKey()+"\t"+newKeys.toString());
        }
    }
    private static int[] findSubVersion(String eval){
        int[] result={-1,-1};
        int matchStart;
        int leftBrace;
        match_loop:
        for(matchStart=1,leftBrace=eval.indexOf('{',matchStart);
        leftBrace>0 && leftBrace+"{c".length()<eval.length();
        leftBrace=eval.indexOf('{',matchStart)){
            int matchedLen=0;
            if (eval.charAt(leftBrace-1)=='$'){
                int subStart=leftBrace+1;
                for (int i = subStart; i < eval.length(); i++) {
                    switch (eval.charAt(i)){
                        case '}':
                            if (matchedLen>0) {
                                result[SUB_START_IDX]=subStart;
                                result[SUB_END_IDX]=subStart+matchedLen;
                                break match_loop;
                            }
                        case ' ':
                        case '$':
                            matchStart=i+1;
                            continue match_loop;
                        default:
                            matchedLen++;
                    }
                }
                break match_loop;
            }else {
                matchStart=leftBrace+1;
            }
        }
        return result;
    }
    public String get(String name){
        String[] names=handleDeprecation(deprecationContext.get(),name);
        String result=null;
        for (String n : names) {
            result=substituteVars(getProps().getProperty(n));
        }
        return result;
    }
    public String get(String name,String defaultValue){
        String[] names=handleDeprecation(deprecationContext.get(),name);
        String result=null;
        for (String n : names) {
            result=substituteVars(getProps().getProperty(n,defaultValue));
        }
        return result;
    }
    public Properties getAllPropertiesByTag(final String tag){
        Properties props = new Properties();
        if (propertyTagsMap.containsKey(tag)) {
            props.putAll(propertyTagsMap.get(tag));
        }
        return props;
    }
    public Properties getAllPropertiesByTags(final List<String > tags){
        Properties props = new Properties();
        for (String tag : tags) {
            props.putAll(this.getAllPropertiesByTag(tag));
        }
        return props;
    }
    private String[] getAlternativeNames(String name){
        String[] altNames=null;
        DeprecatedKeyInfo keyInfo=null;
        DeprecationContext cur=deprecationContext.get();
        String depKey = cur.getRevereDeprecatedKeyMap().get(name);
        if (depKey != null) {
            keyInfo=cur.deprecatedKeyInfoMap.get(depKey);
            if (keyInfo.newKeys.length > 0) {
                if (getProps().containsKey(depKey)) {
                    List<String > list=new ArrayList<>();
                    list.addAll(Arrays.asList(keyInfo.newKeys));
                    list.add(depKey);
                    altNames=list.toArray(new String[list.size()]);
                }
            }else {
                altNames=keyInfo.newKeys;
            }
        }
        return altNames;
    }
    public boolean getBoolean(String name,boolean defaultValue){
        String valueString=getTrimmed(name);
        if (null == valueString || valueString.isEmpty()) {
            return defaultValue;
        }
        if (StringUtils.equalsIgnoreCase("true", valueString)) {
            return true;
        } else if (StringUtils.equalsIgnoreCase("false", valueString)) {
            return false;
        }else {
            LOG.warn("Invalid value for boolean: "+valueString
            +",choose default value: "+defaultValue+" for "+name);
            return defaultValue;
        }
    }
    public <U> Class<? extends U> getClass(String name,Class<? extends U> defaultValue,Class<U> xface){
        try {
            Class<?> theClass=getClass(name,defaultValue);
            if (theClass != null && !xface.isAssignableFrom(theClass)) {
                throw new RuntimeException(theClass+" not "+xface.getName());
            } else if (theClass != null) {
                return theClass.asSubclass(xface);
            }else {
                return null;
            }
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }
    public Class<?> getClass(String name,Class<?> defaultValue){
        String valueString=getTrimmed(name);
        if (valueString == null) {
            return defaultValue;
        }
        try{
            return getClassByname(valueString);
        }catch (ClassNotFoundException e){
            throw new RuntimeException(e);
        }
    }
    public Class<?> getClassByName(String name) throws ClassNotFoundException{
        Class<?> ret=getClassByNameOrNull(name);
        if (ret == null) {
            throw new ClassNotFoundException("Class "+name+" not found");
        }
        return ret;
    }
    public Class<?> getClassByNameOrNull(String name){
        Map<String ,WeakReference<Class<?>>> map;
        synchronized (CACHE_CLASSED){
            map= CACHE_CLASSED.get(classLoader);
            if (map == null) {
                map=Collections.synchronizedMap(
                        new WeakHashMap<>()
                );
                CACHE_CLASSED.put(classLoader,map);
            }
        }
        Class<?> clazz=null;
        WeakReference<Class<?>> ref=map.get(name);
        if (ref != null) {
            clazz=ref.get();
        }
        if (clazz == null) {
            try {
                clazz=Class.forName(name,true,classLoader);
            }catch (ClassNotFoundException e){
                map.put(name,new WeakReference<>(NEGATIVE_CACHE_SENTINEL));
                return null;
            }
            map.put(name,new WeakReference<>(clazz));
            return clazz;
        } else if (clazz == NEGATIVE_CACHE_SENTINEL) {
            return null;
        }else {
            return clazz;
        }
    }
    public Class<?>[] getClasses(String name,Class<?>... defaultValue){
        String valueString=getRaw(name);
        if (valueString == null) {
            return defaultValue;
        }
        String[] classnames=getTrimmedStrings(name);
        try {
            Class<?>[] classes=new Class<?>[classnames.length];
            for (int i = 0; i < classnames.length; i++) {
                classes[i]=getClassByName(classnames[i]);
            }
            return classes;
        }catch (ClassNotFoundException e){
            throw new RuntimeException(e);
        }
    }
    public ClassLoader getClassLoader(){
        return classLoader;
    }
    public InputStream getConfResourceAsInputStream(String name){
        try {
            URL url=getResource(name);
            if (url == null) {
                LOG.info(name+" not found");
                return null;
            }else {
                LOG.info("found resource "+name+" at "+url);
            }
            return url.openStream();
        }catch (Exception e){
            return null;
        }
    }
    public Reader getConfResourceAsReader(String name){
        try{
            URL url=getResource(name);
            if (url == null) {
                LOG.info(name+" not found ");
                return null;
            }else{
                LOG.info("found resource "+name+" at "+url);
            }
            return new InputStreamReader(url.openStream(), StandardCharsets.UTF_8);
        }catch (Exception e){
            return null;
        }
    }
    private CredentialProvider.CredentialEntry getCredentialEntry(CredentialProvider provider,String name){
        CredentialProvider.CredentialEntry entry=provider.getCredentialEntry(name);
        if (entry != null) {
            return entry;
        }
        String oldName=getDeprecatedKey(name);
        if (oldName != null) {
            entry=provider.getCredentialEntry(oldName);
            if (entry != null) {
                logDeprecationOnce(oldName,provider.toString());
                return entry;
            }
        }

        DeprecatedKeyInfo keyInfo=getDeprecatedKeyInfo(name);
        if (keyInfo != null && keyInfo.newKeys != null) {
            for (String newKey : keyInfo.newKeys) {
                entry=provider.getCredentialEntry(newKey);
                if (entry != null) {
                    logDeprecationOnce(name,null);
                    return entry;
                }
            }
        }
        return null;
    }
    private static String getDeprecatedKey(String key){
        return deprecationContext.get().getRevereDeprecatedKeyMap().get(key);
    }
    private static DeprecatedKeyInfo getDeprecatedKeyInfo(String key){
        return deprecationContext.get().deprecatedKeyInfoMap.get(key);
    }
    public double getDouble(String name,double defaultValue){
        String valueString=getTrimmed(name);
        if (valueString == null) {
            return defaultValue;
        }
        return Double.parseDouble(valueString);
    }
    public <T extends Enum<T>> T getEnum(String name,T defaultValue){
        final String val=getTrimmed(name);
        return val==null?defaultValue:Enum.valueOf(defaultValue.getDeclaringClass(),val);
    }
    String getenv(String name){
        if (!restrictSystemProps) {
            return System.getenv(name);
        }else {
            return null;
        }
    }
    public File getFile(String dirsProp,String path) throws IOException {
        String[] dirs=getTrimmedStrings(dirsProp);
        int hashCode=path.hashCode();
        for (int i = 0; i < dirs.length; i++) {
            int index=(hashCode+i & Integer.MAX_VALUE) % dirs.length;
            File file=new File(dirs[index],path);
            File dir=file.getParentFile();
            if (dir.exists() || dir.mkdirs()) {
                return file;
            }
        }
        throw new IOException("No valid local directories in property: "+dirsProp);
    }

    public Set<String > getFinalParameters(){
        Set<String> setFinalParams=Collections.newSetFromMap(new ConcurrentHashMap<>());
        setFinalParams.addAll(finalParameters);
        return setFinalParams;
    }

    public float getFloat(String name,float defaultValue){
        String valueString=getTrimmed(name);
        if (valueString == null) {
            return defaultValue;
        }
        return Float.parseFloat(valueString);
    }

    private String getHexDigits(String value){
        boolean negative=false;
        String str=value;
        String hexString=null;
        if (value.startsWith("-")) {
            negative=true;
            str=value.substring(1);
        }
        if (str.startsWith("0x") || str.startsWith("0X")) {
            hexString=str.substring(2);
            if (negative) {
                hexString="-"+hexString;
            }
            return hexString;
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public <U> List<U> getInstances(String name,Class<U> xface){
        List<U> ret=new ArrayList<>();
        Class<?>[] classes=getClasses(name);
        for (Class<?> aClass : classes) {
            if (!xface.isAssignableFrom(aClass)) {
                throw new RuntimeException(aClass+" does not implement "+xface);
            }
            ret.add((U) ReflectionUtils.newInstance(cl,this));
        }
        return ret;
    }

    public int getInt(String name,int defaultValue){
        String valueString=getTrimmed(name);
        if (valueString == null) {
            return defaultValue;
        }
        String hexString=getHexDigits(valueString);
        if (hexString != null) {
            return Integer.parseInt(hexString,16);
        }
        return Integer.parseInt(valueString);
    }

    public Path getLocalPath(String dirsProp,String path) throws IOException {
        String[] dirs=getTrimmedStrings(dirsProp);
        int hashCode=path.hashCode();
        FileSystem fs=FileSystem.getLocal(this);
        for (int i = 0; i < dirs.length; i++) {
            int index=(hashCode+i & Integer.MAX_VALUE) % dirs.length;
            Path file=new Path(dirs[index],path);
            Path dir=file.getParent();
            if (fs.mkdirs(dir) || fs.exists(dir)) {
                return file;
            }
        }
        LOG.warn("Could not make "+path+" in local directories from "+dirsProp);
        for (int i = 0; i < dirs.length; i++) {
            int index=(hashCode+i & Integer.MAX_VALUE) % dirs.length;
            LOG.warn(dirsProp+"["+index+"]="+dirs[index]);
        }
        throw new IOException("No valid local directories in property: "+dirsProp);
    }

    public long getLong(String name,long defaultValue){
        String valueString = getTrimmed(name);
        if (valueString == null) {
            return defaultValue;
        }
        String hexString = getHexDigits(valueString);
        if (hexString != null) {
            return Long.parseLong(hexString,16);
        }
        return Long.parseLong(hexString);
    }

    public long getLongBytes(String name,long defaultValue){
        String valueString = getTrimmed(name);
        if (valueString == null) {
            return defaultValue;
        }
        return StringUtils.TraditionalBinaryPrefix.string2long(valueString);
    }
    private synchronized Properties getOverlay(){
        if (overlay == null) {
            overlay=new Properties();
        }
        return overlay;
    }
    public char[] getPassword(String name){
        char[] pass=null;
        pass=getPasswordFronCredentialProviders(name);
        if (pass == null) {
            pass=getPasswordFromConfig(name);
        }
        return pass;
    }

    protected char[] getPasswordFromConfig(String name){
        char[] pass=null;
        if (getBoolean(CredentialProvider.CLEAR_TEXT_FALLBACK,
                CommonConfigurationKeys.HADOOP_SECURITY_CREDENTIAL_CLEAR_TEXT_FALLBACK_DEFAULT)) {
            String passStr = get(name);
            if (passStr != null) {
                pass=passStr.toCharArray();
            }
        }
        return pass;
    }

    public char[] getPasswordFromCredentialProviders(String name) throws IOException {
        char[] pass=null;
        try {
            List<CredentialProvider> providers=CredentialProviderFactory.getProviders(this);
            if (providers != null) {
                for (CredentialProvider provider : providers) {
                    try {
                        CredentialProvider.CredentialEntry entry = getCredentialEntry(provider, name);
                        if (entry != null) {
                            pass=entry.getCredential();
                            break;
                        }
                    }catch (IOException e){
                        throw new IOException("Can't get key "+name+" from key provider of type: "+provider.getClass().getName()+".",e);
                    }
                }
            }
        }catch (IOException e){
            throw new IOException("Configuration problem with provider path.",e);
        }
        return pass;
    }
    public Pattern getPattern(String name,Pattern defaultValue){
        String valString = get(name);
        if (valString == null || valString.isEmpty()) {
            return defaultValue;
        }
        try {
            return Pattern.compile(valString);
        }catch (PatternSyntaxException e){
            LOG.warn("Regular expression '"+valString+"' for property '"+name+"' not valid. Using default",e);
            return defaultValue;
        }
    }
    String getProperty(String key){
        if (!restrictSystemProps) {
            return System.getProperty(key);
        }else {
            return null;
        }
    }
    @InterfaceStability.Unstable
    public synchronized String[] getPropertySources(String name){
        if (properties == null) {
            getProps();
        }
        if (properties == null || updatingResources == null) {
            return null;
        }else {
            String[] source = updatingResources.get(name);
            if (source == null) {
                return null;
            }else {
                return Arrays.copyOf(source,source.length);
            }
        }
    }

    protected synchronized Properties getProps(){
        if (properties == null) {
            properties=new Properties();
            loadProps(properties,0,true);
        }
        return properties;
    }

    public Map<String ,String > getPropsWithPrefix(String confPrefix){
        Properties props = getProps();
        Map<String ,String > configMap=new HashMap<>();
        for (String name : props.stringPropertyNames()) {
            if (name.startsWith(confPrefix)) {
                String value = get(name);
                String keyName = name.substring(confPrefix.length());
                configMap.put(keyName,value);
            }
        }
        return configMap;
    }

    synchronized boolean getQuietMode(){return this.quietmode;}
    public IntegerRanges getRanges(String name,String defaultValue){
        return new IntegerRanges(get(name,defaultValue));
    }
    public String getRaw(String name){
        String[] names=handleDeprecation(deprecationContext.get(),name);
        String result=null;
        for (String n : names) {
            result=getProps().getProperty(n);
        }
        return result;
    }
    public URL getResource(String name){
        return classLoader.getResource(name);
    }
    public InetSocketAddress getSocketAddr(
            String name,String defaultAddress,int defaultPort
    ){
        final String address=getTrimmed(name,defaultAddress);
        return NetUtils.createSocketAddr(address,defaultPort,name);
    }

    public InetSocketAddress getSocketAddr(
            String hostProperty,
            String addressProperty,
            String defaultAddressValue,
            int defaultPort
    ){
        InetSocketAddress bindAddr = getSocketAddr(addressProperty, defaultAddressValue, defaultPort);
        final String host=get(hostProperty);
        if (host == null || host.isEmpty()) {
            return bindAddr;
        }
        return NetUtils.createSocketAddr(host,bindAddr.getPort(),hostProperty);
    }

    public double getStorageSize(String name,double defaultValue,StorageUnit targetUnit){
        Preconditions.checkNotNull(targetUnit,"Conversion unit cannot be null.");
        Preconditions.checkState(isNotBlank(name),"Name connot be blank.");
        String vString=get(name);
        if (isBlank(vString)) {
            return targetUnit.getDefault(defaultValue);
        }
        StorageSize measure=StorageSize.parse(vString);
        return convertStorageUnit(measure.getValue(),measure.getUnit(),targetUnit);
    }

    public double getStorageSize(String name,String defaultValue,StorageUnit targetUnit){
        Preconditions.checkState(isNotBlank(name),"Key cannot be blank.");
        String vString=get(name);
        if (isBlank(vString)) {
            vString=defaultValue;
        }
        StorageSize measure=StorageSize.parse(vString);
        return convertStorageUnit(measure.getValue(),measure.getUnit(),targetUnit);
    }
    private XMLStreamReader2 getStreamReader(Resource wrapper,boolean quiet){
        Object resource = wrapper.getResource();
        boolean isRestricted = wrapper.isRestrictParser();
        XMLStreamReader2 reader2=null;
        if (resource instanceof URL){
            reader2=(XMLStreamReader2) parse((URL)resource,isRestricted);
        } else if (resource instanceof String) {
            URL url=getResource((String) resource);
            reader2=(XMLStreamReader2) parse(url,isRestricted);
        }else if (resource instanceof Path){
            File file=new File(((Path)resource).toUri().getPath()).getAbsoluteFile();
            if (file.exists()) {
                if (!quiet) {
                    LOG.debug("parsing File "+file);
                }
                reader2 = (XMLStreamReader2)parse(new BufferedInputStream(
                                Files.newInputStream(file.toPath())), ((Path) resource).toString(),
                        isRestricted);
            }
        } else if (resource instanceof InputStream) {
            reader2=(XMLStreamReader2) parse((InputStream)resource,null,isRestricted);
        }
        return reader2;
    }

    public Collection<String > getStringCollection(String name){
        String valueString=get(name);
        return StringUtils.getStringCollection(valueString);
    }
    public String[] getStrings(String name){
        String valueString = get(name);
        return StringUtils.getStrings(valueString);
    }
    public String[] getStrings(String name,String... defaultValue){
        String valueString = get(name);
        if (valueString == null) {
            return defaultValue;
        }else {
            return StringUtils.getStrings(valueString);
        }
    }

    public long getTimeDuration(String name,long defaultValue,TimeUnit unit){
        return getTimeDuration(name,defaultValue,unit,unit);
    }
    public long getTimeDuration(String name,String defaultValue,TimeUnit unit){
        return getTimeDuration(name,defaultValue,unit,unit);
    }
    public long getTimeDuration(String name,long defaultValue,TimeUnit defaultUnit,TimeUnit returnUnit){
        String v = get(name);
        if (v == null) {
            return returnUnit.convert(defaultValue,defaultUnit);
        }else {
            return getTimeDurationHelper(name,v,defaultUnit,returnUnit);
        }
    }
    public long getTimeDuration(String name,String defaultValue,TimeUnit defaultUnit,TimeUnit returnUnit){
        String v = get(name);
        if (v == null) {
            return getTimeDurationHelper(name,defaultValue,defaultUnit,returnUnit);
        }else {
            return getTimeDurationHelper(name,v,defaultUnit,returnUnit);
        }
    }
    public long getTimeDurationHelper(String name,String v,TimeUnit unit){
        return getTimeDurationHelper(name,v,unit,unit);
    }
    public long getTimeDurationHelper(String name,String v,TimeUnit defaultUnit,TimeUnit returnUnit){
        v=v.trim();
        v=StringUtils.toLowerCase(v);
        ParseTimeDuration vUnit=ParseTimeDuration.unitFor(v);
        if (vUnit == null) {
            vUnit=ParseTimeDuration.unitFor(defaultUnit);
        }else {
            v=v.substring(0,v.lastIndexOf(vUnit.suffix()));
        }
        long raw=Long.parseLong(v);
        long converted=returnUnit.convert(raw,vUnit.unit());
        if (vUnit.unit().convert(converted, returnUnit) < raw) {
            logDeprecation("Possible loss of precision converting "+v+vUnit.suffix()+" to "+returnUnit+" for "+name);
        }
        return converted;
    }
    public long[] getTimeDurations(String name,TimeUnit unit){
        String[] strs=getTrimmedStrings(name);
        long[] durations=new long[strs.length];
        for (int i = 0; i < strs.length; i++) {
            durations[i]=getTimeDurationHelper(name,strs[i],unit);
        }
        return durations;
    }
    public int[] getInts(String name){
        String[] strings=getTrimmedStrings(name);
        int[] ints=new int[strings.length];
        for (int i = 0; i < strings.length; i++) {
            ints[i]=Integer.parseInt(strings[i]);
        }
        return ints;
    }


    public String getTrimmed(String name){
        String value=get(name);
        if (value == null) {
            return null;
        }else {
            return value.trim();
        }
    }

    public String getTrimmed(String name,String defaultValue){
        String ret = getTrimmed(name);
        return ret==null?defaultValue:ret;
    }

    public Collection<String > getTrimmedStringCollection(String name){
        String v = get(name);
        if (v == null) {
            Collection<String > empty=new ArrayList<>();
            return empty;
        }
        return StringUtils.getTrimmedStringCollection(v);
    }
    public String[] getTrimmedStrings(String name){
        String v = get(name);
        return StringUtils.getTrimmedStrings(v);
    }
    public String[] getTrimmedStrings(String name,String ... defaultValue){
        String v = get(name);
        if (v == null) {
            return defaultValue;
        }else {
            return StringUtils.getTrimmedStrings(v);
        }
    }
    public Map<String,String > getValByRegex(String regex){
        Pattern p = Pattern.compile(regex);
        Map<String,String > result=new HashMap<>();
        List<String > resultKeys=new ArrayList<>();
        Matcher m;
        for (Map.Entry<Object, Object> entry : getProps().entrySet()) {
            if (entry.getKey() instanceof String
            && entry.getValue() instanceof String){
                m=p.matcher((String)entry.getKey());
                if (m.find()) {
                    resultKeys.add((String) entry.getKey());
                }
            }
        }
        resultKeys.forEach(key->{
            result.put(key,substituteVars(getProps().getProperty(key)));
        });
        return result;
    }
    private void handleDeprecation(){
        LOG.debug("Handling deprecation for all properties in config...");
        DeprecationContext deprecations = Configuration.deprecationContext.get();
        Set<Object> keys=new HashSet<>();
        keys.addAll(getProps().keySet());
        for (Object key : keys) {
            LOG.debug("Handling deprecation for "+key);
            handleDeprecation(deprecations,(String)key);
        }
    }
    private String[] handleDeprecation(DeprecationContext deprecations,String name){
        if (name != null) {
            name=name.trim();
        }
        String[] names=new String[]{name};
        DeprecatedKeyInfo keyInfo = deprecations.getDeprecatedKeyInfoMap().get(name);
        if (keyInfo != null) {
            if (!keyInfo.getAndSetAccessed()) {
                logDeprecation(keyInfo.getWarningMessage(name));
            }
            names=keyInfo.newKeys;
        }
        updatePropertiesWithDeprecatedKeys(deprecations,names);

        Properties overlayProperties = getOverlay();
        if (overlayProperties.isEmpty()) {
            return names;
        }
        for (String s : names) {
            String deprecatedKey = deprecations.getRevereDeprecatedKeyMap().get(s);
            if (deprecatedKey != null && !overlayProperties.containsKey(s)) {
                String deprecatedValue = overlayProperties.getProperty(deprecatedKey);
                if (deprecatedValue != null) {
                    getProps().setProperty(s,deprecatedValue);
                    overlayProperties.setProperty(s,deprecatedValue);
                }
            }
        }
        return names;
    }
    public static boolean hasWarnedDeprecation(String name){
        DeprecationContext deprecations = Configuration.deprecationContext.get();
        if (deprecations.getDeprecatedKeyInfoMap().containsKey(name)) {
            if (deprecations.getDeprecatedKeyInfoMap().get(name).accessed.get()) {
                return true;
            }
        }
        return false;
    }
    public static boolean isDeprecated(String key){
        return deprecationContext.get().getDeprecatedKeyInfoMap().containsKey(key);
    }
    public boolean isPropertyTag(String tagStr){
        return TAGS.contains(tagStr);
    }
    private void loadProperty(Properties properties,String name,String attr,String value,boolean finalParameter,String[] source){
        if (value != null || allowNullValueProperties) {
            if (value == null) {
                value=DEFAULT_STRING_CHECK;
            }
            if (!finalParameters.contains(attr)) {
                properties.setProperty(attr,value);
                if (source != null) {
                    putIntoUpdatingResource(attr,source);
                }
            }else {
                checkForOverride(this.properties,name,attr,value);
                if (this.properties != properties) {
                    checkForOverride(properties,name,attr,value);
                }
            }
        }
        if (finalParameter && attr != null) {
            finalParameters.add(attr);
        }
    }
    private synchronized void loadProps(final Properties props,
                                        final int startIndex,
                                        final boolean fullReload){
        if (props != null) {
            Map<String ,String []> backup=
                    updatingResources!=null?new ConcurrentHashMap<>(updatingResources):null;
            loadResources(props,resources,startIndex,fullReload,quietmode);
            if (overlay != null) {
                props.putAll(overlay);
                if (backup != null) {
                    for (Map.Entry<Object, Object> entry : overlay.entrySet()) {
                        String key= (String) entry.getKey();
                        String[] source=backup.get(key);
                        if (source != null) {
                            updatingResources.put(key,source);
                        }
                    }
                }
            }
        }
    }
    private Resource loadResource(Properties properties,
                                  Resource wrapper,boolean quiet){
        String name=UNKNOWN_RESOURCE;
        try {
            Object resource=wrapper.getResource();
            name=wrapper.getName();
            boolean returnCachedProperties=false;

            if (resource instanceof InputStream) {
                returnCachedProperties=true;
            } else if (resource instanceof Properties) {
                overlay(properties,(Properties)resource);
            }
            XMLStreamReader2 reader2=getStreamReader(wrapper,quiet);
            if (reader2 == null) {
                if (quiet) {
                    return null;
                }
                throw new RuntimeException(resource+" not found");
            }
            Properties toAddTo=properties;
            if (returnCachedProperties) {
                toAddTo=new Properties();
            }
            List<ParsedItem> items = new Parser(reader2, wrapper, quiet).parse();
            for (ParsedItem item : items) {
                loadProperty(toAddTo,item.name,item.key,item.value,
                        item.isFinal,item.sources);
            }
            reader2.close();
            if (returnCachedProperties) {
                overlay(properties,toAddTo);
                return new Resource(toAddTo,name,wrapper.isRestrictParser());
            }
            return null;
        }catch (IOException|XMLStreamException e){
            LOG.error("error parsing conf "+name,e);
            throw new RuntimeException(e);
        }
    }
    private void loadResources(Properties properties,
                               ArrayList<Resource> resources,
                               int startIdx,
                               boolean fullReload,
                               boolean quiet){
        if (loadDefaults && fullReload){
            for (String resource : defaultResources) {
                loadResource(properties,new Resource(resource,false),quiet);
            }
        }
        for (int i = startIdx; i < resources.size(); i++) {
            Resource ret = loadResource(properties, resources.get(i), quiet);
            if (ret != null) {
                resources.set(i,ret);
            }
        }
        this.addTags(properties);
    }
    @VisibleForTesting
    void loadDeprecation(String message){
        LOG_DEPRECATION.info(message);
    }
    void loadDeprecationOnce(String name,String message){
        DeprecatedKeyInfo keyInfo = getDeprecatedKeyInfo(name);
        if (keyInfo != null && !keyInfo.getAndSetAccessed()) {
            LOG_DEPRECATION.info(keyInfo.getWarningMessage(message));
        }
    }

    @VisibleForTesting
    public boolean onlyKeyExists(String name){
        String[] names = handleDeprecation(deprecationContext.get(), name);
        for (String n : names) {
            if (getProps().getProperty(n, DEFAULT_STRING_CHECK)
                    .equals(DEFAULT_STRING_CHECK)) {
                return true;
            }
        }
        return false;
    }
    private void overlay(Properties to,Properties from){
        synchronized (from){
            for (Map.Entry<Object, Object> entry : from.entrySet()) {
                to.put(entry.getKey(),entry.getValue());
            }
        }
    }
    private XMLStreamReader parse(InputStream is,String systemIdStr,
                                  boolean restricted) throws XMLStreamException {
        if (!quietmode) {
            LOG.debug("parsing input stream "+is);
        }
        if (is == null) {
            return null;
        }
        SystemId systemId = SystemId.construct(systemIdStr);
        ReaderConfig readerConfig = XML_INPUT_FACTORY.createPrivateConfig();
        if (restricted) {
            readerConfig.setProperty(XMLInputFactory.SUPPORT_DTD,false);
        }
        return XML_INPUT_FACTORY.createSR(readerConfig,systemId,
                StreamBootstrapper.getInstance(null,systemId,is),false,true);
    }
    private XMLStreamReader parse(URL url,boolean restricted) throws IOException, XMLStreamException {
        if (!quietmode) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("parsing URL "+url);
            }
        }
        if (url == null) {
            return null;
        }
        URLConnection connection = url.openConnection();
        if (connection instanceof JarURLConnection) {
            connection.setUseCaches(false);
        }
        return parse(connection.getInputStream(),url.toString(),restricted);
    }
    private void putIntoUpdatingResource(String key,String[] value){
        Map<String, String[]> localUR = this.updatingResources;
        if (localUR == null) {
            synchronized (this){
                localUR=updatingResources;
                if (localUR == null) {
                    updatingResources=localUR=new ConcurrentHashMap<>(8);
                }
            }
        }
        localUR.put(key,value);
    }
    private void readTagFromConfig(String attributeValue,
                                   String configName,
                                   String configValue,
                                   String[] confSource){
        for (String tagStr : attributeValue.split(",")) {
            try {
                tagStr=tagStr.trim();
                if (configValue == null) {
                    configValue="";
                }
                if (propertyTagsMap.containsKey(tagStr)) {
                    propertyTagsMap.get(tagStr).setProperty(configName,configValue);
                }else {
                    Properties props = new Properties();
                    props.setProperty(configName,configValue);
                    propertyTagsMap.put(tagStr,props);
                }
            }catch (Exception e){
                LOG.trace("Tag '{}' for property:{} Source:{}",tagStr,
                        configName,confSource,e);
            }
        }
    }
    public synchronized void reloadConfiguration(){
        properties=null;
        finalParameters.clear();
    }
    public static synchronized void reloadExistingConfigurations(){
        if (LOG.isDebugEnabled()) {
            LOG.debug("Reloading "+REGISTRY.keySet().size()
            +" existing configurations");
        }
        for (Configuration conf : REGISTRY.keySet()) {
            conf.reloadConfiguration();
        }
    }
    public void set(String name,String value){
        set(name,value,null);
    }
    public void set(String name,String value,String source){
        Preconditions.checkArgument(name!=null,
                "Property name must not be null");
        Preconditions.checkArgument(value!=null,
                "The value property %s must not be null",name);
        name=name.trim();
        DeprecationContext deprecations = Configuration.deprecationContext.get();
        if (deprecations.getRevereDeprecatedKeyMap().isEmpty()) {
            getProps();
        }
        getOverlay().setProperty(name,value);
        getProps().setProperty(name,value);
        String newSource=(source==null?"programmatically":source);
        if (!isDeprecated(name)) {
            putIntoUpdatingResource(name,new String[]{newSource});
            String[] altNames = getAlternativeNames(name);
            if (altNames != null) {
                for (String altName : altNames) {
                    if (!altName.equals(name)) {
                        getOverlay().setProperty(altName,value);
                        getProps().setProperty(altName,value);
                        putIntoUpdatingResource(altName,new String[]{newSource});
                    }
                }
            }
        }else {
            String[] names = handleDeprecation(deprecationContext.get(), name);
            String altSource="because "+name+" is deprecated";
            for (String n : names) {
                getOverlay().setProperty(n,value);
                getProps().setProperty(n,value);
                putIntoUpdatingResource(n,new String[]{altSource});
            }
        }
    }
    @VisibleForTesting
    public void setAllowNullValueProperties(boolean value){
        this.allowNullValueProperties=value;
    }
    public void setBoolean(String name,boolean value){
        set(name,Boolean.toString(value));
    }
    public void setBooleanIfUnset(String name,boolean value){
        setIfUnset(name,Boolean.toString(value));
    }
    public void setClass(String name,Class<?> theClass,Class<?> xface){
        if (!xface.isAssignableFrom(theClass)) {
            throw new RuntimeException(theClass+" not "+xface.getName());
        }
        set(name,theClass.getName());
    }
    public void setClassLoader(ClassLoader classLoader){
        this.classLoader=classLoader;
    }
    public void setDeprecatedProperties(){
        DeprecationContext deprecations = Configuration.deprecationContext.get();
        Properties props = getProps();
        Properties overlay = getOverlay();
        for (Map.Entry<String, DeprecatedKeyInfo> entry : deprecations.getDeprecatedKeyInfoMap().entrySet()) {
            String depKey=entry.getKey();
            if (!overlay.contains(depKey)) {
                for (String newKey : entry.getValue().newKeys) {
                    String val = overlay.getProperty(newKey);
                    if (val != null) {
                        props.setProperty(depKey,val);
                        overlay.setProperty(depKey,val);
                        break;
                    }
                }
            }
        }
    }
    public void setDouble(String name,double value){
        set(name,Double.toString(value));
    }
    public <T extends Enum<T>> void setEnum(String name,T value){
        set(name,value.toString());
    }
    public void setFloat(String name,float value){
        set(name,Float.toString(value));
    }
    public synchronized void setIfUnset(String name,String value){
        if (get(name)==null) {
            set(name,value);
        }
    }
    public void setInt(String name,int value){
        set(name,Integer.toString(value));
    }
    public void setLong(String name,long value){
        set(name,Long.toString(value));
    }
    public void setPattern(String name,Pattern pattern){
        assert pattern!=null : "Pattern cannot be null";
        set(name,pattern.pattern());
    }
    public synchronized void setQuietmode(boolean quietmode){
        this.quietmode=quietmode;
    }
    public void setRestrictSystemProps(boolean val){
        this.restrictSystemProps=val;
    }
    public void setRestrictSystemPropsDefault(boolean val){
        restrictSystemPropsDefault=val;
    }
    public void setSocketAddr(String name,InetSocketAddress address){
        set(name,NetUtils.getHostPortString(address));
    }
    public void setStorageSize(String name,double value,StorageUnit unit){
        set(name,value+unit.getShortName());
    }
    public void setStrings(String name,String ... values){
        set(name,StringUtils.arrayToString(values));
    }
    public void setTimeDuration(String name,long value,TimeUnit unit){
        set(name,value+ParseTimeDuration.unitFor(unit).suffix());
    }
    public int size(){
        return getProps().size();
    }
    public String substituteCommonVariables(String expr){
        return substituteVars(expr);
    }
    private String substituteVars(String expr){
        if (expr == null) {
            return null;
        }
        String eval=expr;
        for (int i = 0; i < MAX_SUBST; i++) {
            final int[] varBounds=findSubVersion(eval);
            if (varBounds[SUB_START_IDX] == -1) {
                return eval;
            }
            final String var=eval.substring(varBounds[SUB_START_IDX]
            ,varBounds[SUB_END_IDX]);
            String val=null;
            try {
                if (var.startsWith("env.") && 4 < var.length()) {
                    String v = var.substring(4);
                    int j=0;
                    for (;j<v.length();j++){
                        char c=v.charAt(j);
                        if (c==':'&&j<v.length()-1&&v.charAt(j+1)=='-'){
                            val=getenv(v.substring(0,j));
                            if (val == null || val.length() == 0) {
                                val=v.substring(j+2);
                            }
                            break;
                        }else if (c=='-'){
                            val=getenv(v.substring(0,j));
                            if (val == null) {
                                val=v.substring(i+1);
                            }
                            break;
                        }
                    }
                    if (i == v.length()) {
                        val=getenv(v);
                    }
                }else {
                    val=getProperty(var);
                }
            }catch (SecurityException e){
                LOG.warn("Unexpected SecurityException in Configuration",e);
            }
            if (val == null) {
                val=getRaw(var);
            }
            if (val == null) {
                return eval;
            }
            final int dollar=varBounds[SUB_START_IDX]-"${".length();
            final int afterRightBrace=varBounds[SUB_END_IDX]+"}".length();
            final String refVar=eval.substring(dollar,afterRightBrace);
            if (val.contains(refVar)) {
                return expr;
            }
            eval=eval.substring(0,dollar)
                    +val
                    +eval.substring(afterRightBrace);
        }
        throw new IllegalArgumentException("Variable substitution depth too large: "
        +MAX_SUBST+" "+expr);
    }
    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("Configuration: ");
        if (loadDefaults) {
            toString(defaultResources,sb);
            if (resources.size()>0) {
                sb.append(", ");
            }
        }
        toString(resources,sb);
        return sb.toString();
    }

    private <T> void toString(List<T> resources,StringBuilder sb){
        ListIterator<T> i = resources.listIterator();
        while (i.hasNext()) {
            if (i.nextIndex() != 0) {
                sb.append(", ");
            }
            sb.append(i.next());
        }
    }
    public synchronized void unset(String name){
        String[] names=null;
        if (!isDeprecated(name)) {
            names=getAlternativeNames(name);
            if (names == null) {
                names=new String[]{name};
            }
        }else {
            names=handleDeprecation(deprecationContext.get(),name);
        }
        for (String n : names) {
            getOverlay().remove(n);
            getProps().remove(n);
        }
    }
    public InetSocketAddress updateConnectAddr(String name,
                                               InetSocketAddress newAddr){
        final InetSocketAddress connectAddr=NetUtils.getConnectAddress(newAddr);
        setSocketAddr(name,connectAddr);
        return connectAddr;
    }
    public InetSocketAddress updateConnectAddr(
            String hostProperty,
            String addressProperty,
            String defaultAddressValue,
            InetSocketAddress addr
    ){
        final String host=get(hostProperty);
        final String connectHostPort=getTrimmed(addressProperty,defaultAddressValue);

        if (host == null || host.isEmpty() || connectHostPort == null || connectHostPort.isEmpty()) {
            return updateConnectAddr(addressProperty,addr);
        }
        final String connectHost=connectHostPort.split(":")[0];
        return updateConnectAddr(addressProperty,NetUtils.createSocketAddrForHost(
                connectHost,addr.getPort()
        ));
    }
    private void updatePropertiesWithDeprecatedKeys(
            DeprecationContext deprecations,String[] newNames
    ){
        for (String newName : newNames) {
            String deprecatedKey = deprecations.getRevereDeprecatedKeyMap().get(newName);
            if (deprecatedKey != null && !getProps().containsKey(newName)) {
                String deprecatedValue = getProps().getProperty(deprecatedKey);
                if (deprecatedValue != null) {
                    getProps().setProperty(newName,deprecatedValue);
                }
            }
        }
    }
    public void writeXml(OutputStream out) throws IOException {
        writeXml(new OutputStreamWriter(out,StandardCharsets.UTF_8));
    }
    public void writeXml(Writer out) throws IOException {
        writeXml(null,out);
    }
    public void writeXml(@Nullable String propertyName,Writer out) throws IOException {
        writeXml(propertyName,out,null);
    }
    public void writeXml(@Nullable String propertyName,Writer out,Configuration conf) throws IOException {
        ConfigRedactor redactor=conf!=null?new ConfigRedactor(this):null;
        Document doc=asXmlDocument(propertyName,redactor);
        try {
            DOMSource source = new DOMSource(doc);
            StreamResult result = new StreamResult(out);
            TransformerFactory transformerFactory= XMLUtils.newSecureTransformerFactory();
            Transformer transformer=transformerFactory.newTransformer();
            transformer.transform(source,result);
        }catch (TransformerException e){
            throw new IOException(e);
        }
    }


    public static void main(String[] args) {
        new Configuration().writeXml(System.out);
    }
    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        Properties props=getProps();
        Map<String ,String > result=new HashMap<>();
        synchronized (props){
            for (Map.Entry<Object, Object> item : props.entrySet()) {
                if (item.getKey() instanceof String && item.getValue() instanceof String){
                    result.put((String) item.getKey(),(String) item.getValue());
                }
            }
        }
        return result.entrySet().iterator();
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        clear();
        int size= WritableUtils.readVInt(in);
        for (int i = 0; i < size; i++) {
            String key= Text.readString(in);
            String value=Text.readString(in);
            set(key,value);
            String resources[]=WritableUtils.readCompressedStringArray(in);
            if (resources != null) {
                putIntoUpdatingResource(key,resources);
            }
        }
    }

    @Override
    public void writer(DataOutput out) throws IOException {
        Properties props=getProps();
        WritableUtils.writeVInt(out,props.size());
        for (Map.Entry<Object, Object> item : props.entrySet()) {
            Text.writeString(out,(String)item.getKey());
            Text.writeString(out,(String) item.getValue());
            WritableUtils.writeCompressedStringArray(out,updatingResources!=null?
                    updatingResources.get(item.getKey()):null);
        }
    }



    private static class DeprecatedKeyInfo{
        private final String[] newKeys;
        private final String customMessage;
        private final AtomicBoolean accessed=new AtomicBoolean(false);
        DeprecatedKeyInfo(String[] newKeys,String customMessage){
            this.newKeys=newKeys;
            this.customMessage=customMessage;
        }
        private final String getWarningMessage(String key){
            return getWarningMessage(key,null);
        }
        private String getWarningMessage(String key,String source){
            String warningMessage;
            if (customMessage == null) {
                StringBuilder message = new StringBuilder(key);
                if (source != null) {
                    message.append(" in "+source);
                }
                message.append(" is deprecated. Instead, use ");
                for (int i = 0; i < newKeys.length; i++) {
                    message.append(newKeys[i]);
                    if (i!=newKeys.length-1){
                        message.append(", ");
                    }
                }
                warningMessage=message.toString();
            }else {
                warningMessage=customMessage;
            }
            return warningMessage;
        }
        boolean getAndSetAccessed(){
            return accessed.getAndSet(true);
        }
        public void clearAccessed(){
            accessed.set(false);
        }
    }

    private static class DeprecationContext{
        private final Map<String ,DeprecatedKeyInfo> deprecatedKeyInfoMap;
        private final Map<String ,String > revereDeprecatedKeyMap;
        @SuppressWarnings("unchecked")
        DeprecationContext(DeprecationContext other,DeprecationDelta[] deltas){
            Map<String ,DeprecatedKeyInfo> newDeprecatedKeyMap=new HashMap<>();
            Map<String ,String > newReverseDeprecatedKeyMap=new HashMap<>();
            if (other != null) {
                for (Map.Entry<String, DeprecatedKeyInfo> entry : other.deprecatedKeyInfoMap.entrySet()) {
                    newDeprecatedKeyMap.put(entry.getKey(),entry.getValue());
                }
                for (Map.Entry<String, String> entry : other.revereDeprecatedKeyMap.entrySet()) {
                    newReverseDeprecatedKeyMap.put(entry.getKey(), entry.getValue());
                }
            }
            for (DeprecationDelta delta : deltas) {
                if (!newDeprecatedKeyMap.containsKey(delta.getKey())) {
                    DeprecatedKeyInfo newKeyInfo = new DeprecatedKeyInfo(delta.getNewKeys(), delta.getCustomMessage());
                    newDeprecatedKeyMap.put(delta.key,newKeyInfo);
                    for (String newKey : delta.getNewKeys()){
                        newReverseDeprecatedKeyMap.put(newKey,delta.key);
                    }
                }
            }
            this.deprecatedKeyInfoMap= UnmodifiableMap.decorate(newDeprecatedKeyMap );
            this.revereDeprecatedKeyMap=UnmodifiableMap.decorate(newReverseDeprecatedKeyMap);
        }
        Map<String ,DeprecatedKeyInfo> getDeprecatedKeyInfoMap(){return deprecatedKeyInfoMap;}
        Map<String ,String > getRevereDeprecatedKeyMap(){return revereDeprecatedKeyMap;}
    }
    public static class DeprecationDelta{
        private final String key;
        private final String[] newKeys;
        private final String customMessage;
        DeprecationDelta(String key,String[] newKeys,String customMessage){
            Preconditions.checkNotNull(key);
            Preconditions.checkNotNull(newKeys);
            Preconditions.checkArgument(newKeys.length>0);
            this.key=key;
            this.newKeys=newKeys;
            this.customMessage=customMessage;
        }
        public DeprecationDelta(String key,String newKey,String customMessage){
            this(key,new String[]{newKey},customMessage);
        }
        public DeprecationDelta(String key,String newKey){
            this(key,new String[]{newKey},null);
        }

        public String getKey() {
            return key;
        }

        public String[] getNewKeys() {
            return newKeys;
        }

        public String getCustomMessage() {
            return customMessage;
        }
    }
    public static class IntegerRanges implements Iterable<Integer>{
        List<Range> ranges=new ArrayList<>();
        public IntegerRanges(){}
        public IntegerRanges(String newValue){
            StringTokenizer itr = new StringTokenizer(newValue, ",");
            while (itr.hasMoreTokens()) {
                String rng = itr.nextToken().trim();
                String[] parts = rng.split("-", 3);
                if (parts.length<1 || parts.length>2){
                    throw new IllegalArgumentException("integer range badly formed: "+rng);
                }
                Range r=new Range();
                r.start=convertToInt(parts[0],0);
                if (parts.length == 2) {
                    r.end=convertToInt(parts[1],Integer.MAX_VALUE);
                }else {
                    r.end=r.start;
                }
                if (r.start > r.end) {
                    throw new IllegalArgumentException("IntegerRange from "+r.start+" to "+r.end+" is invalid");
                }
                ranges.add(r);
            }
        }
        private static int convertToInt(String value,int defaultValue){
            String trim = value.trim();
            if (trim.length() == 0) {
                return defaultValue;
            }
            return Integer.parseInt(trim);
        }
        public boolean isIncluded(int value){
            for (Range range : ranges) {
                if (range.start<=value && value<=range.end){
                    return true;
                }
            }
            return false;
        }
        public boolean isEmpty(){return ranges==null || ranges.isEmpty();}

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            boolean first=true;
            for (Range r : ranges) {
                if (first) {
                    first=false;
                }else {
                    sb.append(',');
                }
                sb.append(r.start)
                        .append('-')
                        .append(r.end);
            }
            return sb.toString();
        }

        public int getRangeStart(){
            if (isEmpty()) {
                return -1;
            }
            Range r = ranges.get(0);
            return r.start;
        }

        @Override
        public Iterator<Integer> iterator() {
            return new RangeNumberIterator(ranges);
        }

        private static class Range{
            int start;
            int end;
        }
        private static class RangeNumberIterator implements Iterator<Integer>{
            Iterator<Range> interval;
            int at;
            int end;
            public RangeNumberIterator(List<Range> ranges){
                if (ranges != null) {
                    interval=ranges.iterator();
                }
                at=-1;
                end=-2;
            }

            @Override
            public boolean hasNext() {
                if (at <= end) {
                    return true;
                } else if (interval != null) {
                    return interval.hasNext();
                }
                return false;
            }

            @Override
            public Integer next() {
                if (at <= end) {
                    at++;
                    return at+1;
                } else if (interval != null) {
                    Range found = interval.next();
                    if (found != null) {
                        at=found.start;
                        end=found.end;
                        at++;
                        return at-1;
                    }
                }
                return null;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        }
    }
    private static abstract class NegativeCacheSentinel{}
    private static class ParsedItem{
        String name;
        String key;
        String value;
        boolean isFinal;
        String[] sources;

        public ParsedItem(String name, String key, String value, boolean isFinal, String[] sources) {
            this.name = name;
            this.key = key;
            this.value = value;
            this.isFinal = isFinal;
            this.sources = sources;
        }
    }
    enum ParseTimeDuration{
        NS{
            TimeUnit unit(){return TimeUnit.NANOSECONDS;}
            String suffix() {  return  "ns";}
        },
        US{
            TimeUnit unit(){return TimeUnit.MICROSECONDS;}
            String suffix(){return "us";}
        },
        MS{
            TimeUnit unit(){return TimeUnit.MILLISECONDS;}
            String suffix(){return "ms";}
        },
        S{
            TimeUnit unit(){return TimeUnit.SECONDS;}
            String suffix(){return "s";}
        },
        M{
            TimeUnit unit(){return TimeUnit.MINUTES;}
            String suffix(){return "m";}
        },
        H{
            TimeUnit unit(){return TimeUnit.HOURS;}
            String suffix(){return "h";}
        },
        D{
            TimeUnit unit(){return TimeUnit.DAYS;}
            String suffix(){return "d";}
        };
        abstract TimeUnit unit();
        abstract String suffix();
        static ParseTimeDuration unitFor(String s){
            for (ParseTimeDuration ptd : values()) {
                if (s.endsWith(ptd.suffix())) {
                    return ptd;
                }
            }
            return null;
        }
        static ParseTimeDuration unitFor(TimeUnit unit){
            for (ParseTimeDuration ptd : values()) {
                if (ptd.unit() == unit) {
                    return ptd;
                }
            }
            return null;
        }
    }
    private class Parser{
        private final XMLStreamReader2 reader2;
        private final Resource wrapper;
        private final String name;
        private final String[] nameSingletonArray;
        private final boolean isRestricted;
        private final boolean quiet;
        DeprecationContext deprecations=deprecationContext.get();
        private StringBuilder token=new StringBuilder();
        private String confName=null;
        private String confValue=null;
        private String confInclude=null;
        private String confTag=null;
        private boolean confFinal=false;
        private boolean fallbackAllowed=false;
        private boolean fallbackEntered=false;
        private boolean parseToken=false;
        private List<String > confSource=new ArrayList<>();
        private List<ParsedItem> results=new ArrayList<>();

        Parser(XMLStreamReader2 reader2,Resource wrapper,boolean quiet){
            this.reader2=reader2;
            this.wrapper=wrapper;
            this.name=wrapper.getName();
            this.nameSingletonArray=new String[]{name};
            this.isRestricted=wrapper.isRestrictParser();
            this.quiet=quiet;
        }
        void handleEndElement() throws IOException {
            String tokenStr = token.toString();
            switch (reader2.getLocalName()){
                case "name":
                    if (token.length() > 0) {
                        confName= StringInterner.weakIntern(tokenStr.trim());
                    }
                    break;
                case "value":
                    if (token.length()>0) {
                        confValue=StringInterner.weakIntern(tokenStr);
                    }
                    break;
                case "final":
                    confFinal="true".equals(tokenStr);
                    break;
                case "source":
                    confSource.add(StringInterner.weakIntern(tokenStr));
                    break;
                case "tag":
                    if (token.length() > 0) {
                        confTag=StringInterner.weakIntern(tokenStr);
                    }
                    break;
                case "include":
                    if (fallbackAllowed && !fallbackEntered) {
                        throw new IOException("Fetch fail on include for '"+confInclude
                        +"' with no fallback while loading '"+name+"'");
                    }
                    fallbackAllowed=false;
                    fallbackEntered=false;
                    break;
                case "property":
                    handleEndProperty();
                    break;
                default:
                    break;
            }
        }
        void handleEndProperty(){
            if (confName == null || (!fallbackAllowed && fallbackEntered)) {
                return;
            }
            String[] confSourceArray;
            if (confSource.isEmpty()) {
                confSourceArray=nameSingletonArray;
            }else {
                confSource.add(name);
                confSourceArray=confSource.toArray(new String[0]);
            }
            if (confTag != null) {
                readTagFromConfig(confTag,confName,confValue,confSourceArray);
            }
            DeprecatedKeyInfo keyInfo=deprecations.getDeprecatedKeyInfoMap().get(confName);
            if (keyInfo != null) {
                keyInfo.clearAccessed();
                for (String key : keyInfo.newKeys) {
                    results.add(new ParsedItem(
                            name,key,confValue,confFinal,confSourceArray
                    ));
                }
            }else {
                results.add(new ParsedItem(
                        name,confName,confValue,confFinal,confSourceArray
                ));
            }
        }
        protected void handleInclude() throws XMLStreamException,IOException {
            confInclude=null;
            int attrCount = reader2.getAttributeCount();
            List<ParsedItem> items;
            for (int i = 0; i < attrCount; i++) {
                String attrName = reader2.getAttributeLocalName(i);
                if ("href".equals(attrName)) {
                    confInclude=reader2.getAttributeValue(i);
                }
            }
            if (confInclude == null) {
                return;
            }
            if (isRestricted) {
                throw new RuntimeException("Error parsing resource "+wrapper
                +": XInclude is not supported for restricted resources");
            }
            URL include=getResource(confInclude);
            if (include != null) {
                Resource classpathResource=new Resource(include,name,wrapper.isRestrictParser());
                synchronized (Configuration.this){
                    XMLStreamReader2 includeReader=getStreamReader(classpathResource,quiet);
                    if (includeReader == null) {
                        throw new RuntimeException(classpathResource+" not found");
                    }
                    items=new Parser(includeReader,classpathResource,quiet).parse();
                }
            }else {
                URL url;
                try{
                    url=new URL(confInclude);
                    url.openConnection().connect();
                }catch (IOException e){
                    File href = new File(confInclude);
                    if (!href.isAbsolute()) {
                        File baseFile;
                        try {
                            baseFile=new File(new URI(name));
                        } catch (URISyntaxException uriSyntaxException) {
                            baseFile=new File(name);
                        }
                        baseFile=baseFile.getParentFile();
                        href=new File(baseFile,href.getPath());
                    }
                    if (!href.exists()) {
                        fallbackAllowed=true;
                        return;
                    }
                    url=href.toURI().toURL();
                }
                Resource uriResource=new Resource(url,name,wrapper.isRestrictParser());
                synchronized (Configuration.this){
                    XMLStreamReader2 includeReader=getStreamReader(uriResource,quiet);
                    if (includeReader == null) {
                        throw new RuntimeException(uriResource+" not found");
                    }
                    items=new Parser(includeReader,uriResource,quiet).parse();
                }
            }
            results.addAll(items);
        }
        private void handleStartElement() throws XMLStreamException, IOException {
            switch (reader2.getLocalName()){
                case "property":
                    handleStartProperty();
                    break;
                case "name":
                case "value":
                case "final":
                case "source":
                case "tag":
                    parseToken=true;
                    token.setLength(0);
                    break;
                case "include":
                    handleInclude();
                    break;
                case "fallback":
                    fallbackEntered=true;
                    break;
                case "configuration":
                    break;
                default:break;
            }
        }
        private void handleStartProperty(){
            confName=null;
            confValue=null;
            confFinal=false;
            confTag=null;
            confSource.clear();
            int attrCount = reader2.getAttributeCount();
            for (int i = 0; i < attrCount; i++) {
                String propertyAttr = reader2.getAttributeLocalName(i);
                if ("name".equals(propertyAttr)) {
                    confName=StringInterner.weakIntern(reader2.getAttributeValue(i));
                } else if ("value".equals(propertyAttr)) {
                    confValue=StringInterner.weakIntern(reader2.getAttributeValue(i));
                } else if ("final".equals(propertyAttr)) {
                    confFinal="true".equals(reader2.getAttributeValue(i));
                } else if ("source".equals(propertyAttr)) {
                    confSource.add(StringInterner.weakIntern(
                            reader2.getAttributeValue(i)
                    ));
                } else if ("tag".equals(propertyAttr)) {
                    confTag=StringInterner.weakIntern(reader2.getAttributeValue(i));
                }
            }
        }
        List<ParsedItem> parse() throws XMLStreamException, IOException {
            while (reader2.hasNext()) {
                parseNext();
            }
            return results;
        }
        void parseNext() throws XMLStreamException, IOException {
            switch (reader2.next()){
                case XMLStreamConstants.START_ELEMENT:
                    handleStartElement();
                    break;
                case XMLStreamConstants.CHARACTERS:
                case XMLStreamConstants.CDATA:
                    if (parseToken) {
                        char[] text = reader2.getTextCharacters();
                        token.append(text,reader2.getTextStart(),reader2.getTextLength());
                    }
                    break;
                case XMLStreamConstants.END_ELEMENT:
                    handleEndElement();
                    break;
                default:
                    break;
            }
        }
    }
    private static class Resource{
        private final Object resource;
        private final String name;
        private final boolean restrictParser;
        public Resource(Object resource){this(resource,resource.toString());}
        public Resource(Object resource,boolean useRestrictedParser){
            this(resource,resource.toString(),useRestrictedParser);
        }
        public Resource(Object resource,String name){this(resource,name,getRestrictParserDefault(resource));}
        public Resource(Object resource, String name, boolean restrictParser) {
            this.resource = resource;
            this.name = name;
            this.restrictParser = restrictParser;
        }

        public String getName() {
            return name;
        }

        public Object getResource() {
            return resource;
        }

        public boolean isRestrictParser() {
            return restrictParser;
        }

        @Override
        public String toString() {
            return name;
        }
        private static boolean getRestrictParserDefault(Object resource){
            if (resource instanceof String || !UserGroupInformation.isInitialized()){
                return false;
            }
            UserGroupInformation user;
            try {
                user=UserGroupInformation.getCurrentUser();
            }catch (IOException e){
                throw new RuntimeException("Unable to determine current user",e);
            }
            return user.getRealUser()!=null;
        }
    }
}
