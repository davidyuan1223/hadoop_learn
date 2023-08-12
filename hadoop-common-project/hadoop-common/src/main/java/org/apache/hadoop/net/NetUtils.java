package org.apache.hadoop.net;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.thirdparty.com.google.common.cache.Cache;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.thirdparty.com.google.common.net.InetAddresses;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@InterfaceAudience.LimitedPrivate({"HDFS","MapReduce"})
@InterfaceStability.Unstable
public class NetUtils {
    private static final Logger LOG= LoggerFactory.getLogger(NetUtils.class);
    private static Map<String ,String > hostToResolved=new HashMap<>();
    private static final String FOR_MORE_DETAILS_SEE="For more details see";
    public static final String UNKNOWN_HOST="(unknown)";
    public static final String HADOOP_WIKI="http://wiki.apache.org/hadoop/";
    public static SocketFactory getSocketFactory(Configuration conf,Class<?> clazz){
        SocketFactory factory=null;
        String propValue = conf.get("hadoop.rpc.socket.factory.class." + clazz.getSimpleName());
        if (propValue != null && propValue.length() > 0) {
            factory=getSocketFactoryFromProperty(conf,propValue);
        }
        if (factory == null) {
            factory=getDefaultSocketFactory(conf);
        }
        return factory;
    }

    public static SocketFactory getDefaultSocketFactory(Configuration conf){
        String propValue = conf.get(
                CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY,
                CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_DEFAULT
        );
        if (propValue == null || propValue.length() == 0) {
            return SocketFactory.getDefault();
        }
        return getSocketFactoryFromProperty(conf,propValue);
    }

    public static SocketFactory getSocketFactoryFromProperty(Configuration conf, String propValue) {
        try {
            Class<?> theClass=conf.getClassByName(propValue);
            return (SocketFactory) ReflectionUtils.newInstance(theClass,conf);
        }catch (ClassNotFoundException e){
            throw new RuntimeException("Socket Factory class not found: "+e);
        }
    }

    public static InetSocketAddress createSocketAddr(String target){
        return createSocketAddr(target,-1);
    }

    public static InetSocketAddress createSocketAddr(String target,int defaultPort){
        return createSocketAddr(target,defaultPort,null);
    }
    public static InetSocketAddress createSocketAddr(String target,
                                                     int defaultPort,
                                                     String configName) {
        return createSocketAddr(target, defaultPort, configName, false);
    }
    public static InetSocketAddress createSocketAddr(String target,int defaultPort,
                                                     String confName,boolean useCacheIfPresent){
        String helpText="";
        if (confName != null) {
            helpText=" (configuration property '"+confName+"')";
        }
        if (target == null) {
            throw new IllegalArgumentException("Target address cannot be null."+helpText);
        }
        target=target.trim();
        boolean hasScheme = target.contains("://");
        URI uri=createURI(target,hasScheme,helpText,useCacheIfPresent);
        String host = uri.getHost();
        int port = uri.getPort();
        if (port==-1) {
            port=defaultPort;
        }
        String path = uri.getPath();
        if (host == null || port < 0 || (!hasScheme && path != null && !path.isEmpty())) {
            throw new IllegalArgumentException("Does not contain a valid host:port authority: "+target+helpText);
        }
        return createSocketAddrForHost(host,port);
    }

    private static final long URI_CACHE_SIZE_DEFAULT=1000;
    private static final long URI_CACHE_EXPIRE_TIME_DEFAULT=12;
    private static final Cache<String ,URI> URI_CACHE= CacheBuilder.newBuilder()
            .maximumSize(URI_CACHE_SIZE_DEFAULT)
            .expireAfterWrite(URI_CACHE_EXPIRE_TIME_DEFAULT, TimeUnit.HOURS)
            .build();

    private static URI createURI(String target,boolean hasScheme,String helpText,boolean useCacheIfPresent){
        URI uri;
        if (useCacheIfPresent) {
            uri=URI_CACHE.getIfPresent(target);
            if (uri != null) {
                return uri;
            }
        }
        try {
            uri=hasScheme?URI.create(target):URI.create("dummyscheme://"+target);
        }catch (IllegalArgumentException e){
            throw new IllegalArgumentException("Does not contain a valid host:port authority: "+target+helpText);
        }
        if (useCacheIfPresent) {
            URI_CACHE.put(target,uri);
        }
        return uri;
    }

    public static InetSocketAddress createSocketAddrForHost(String host,int port){
        String staticHost=getStaticResolution(host);
        String resolveHost=(staticHost!=null)?staticHost:host;
        InetSocketAddress addr;
        try {
            InetAddress iaddr= SecurityUtil.getByName(resolveHost);
            if (staticHost != null) {
                iaddr=InetAddress.getByAddress(host,iaddr.getAddress());
            }
            addr=new InetSocketAddress(iaddr,port);
        }catch (UnknownHostException e){
            addr=InetSocketAddress.createUnresolved(host,port);
        }
        return addr;
    }

    public static URI getCanonicalUri(URI uri,int defaultPort){
        String host = uri.getHost();
        if (host == null) {
            return uri;
        }
        String fqHost=canonicalizeHost(host);
        int port = uri.getPort();
        if (host.equals(fqHost) && port != -1) {
            return uri;
        }
        try {
            uri=new URI(uri.getScheme(),uri.getUserInfo(),fqHost,(port==-1)?defaultPort:port,
                    uri.getPath(),uri.getQuery(),uri.getFragment());
        }catch (URISyntaxException e){
            throw new IllegalArgumentException(e);
        }
        return uri;
    }
    private static final ConcurrentHashMap<String ,String > canonicalizedHostCache=new ConcurrentHashMap<>();
    private static String canonicalizeHost(String host){
        String fqHost = canonicalizedHostCache.get(host);
        if (fqHost == null) {
            try {
                fqHost=SecurityUtil.getByName(host).getHostName();
                canonicalizedHostCache.putIfAbsent(host,fqHost);
                fqHost=canonicalizedHostCache.get(host);
            }catch (UnknownHostException e){
                fqHost=host;
            }
        }
        return fqHost;
    }
    public static void addStaticResolution(String host,String resolvedName){
        synchronized (hostToResolved){
            hostToResolved.put(host,resolvedName);
        }
    }
    public static String getStaticResolution(String host){
        synchronized (hostToResolved){
            return hostToResolved.get(host);
        }
    }

    public static List<String[]> getAllStaticResolutions(){
        synchronized (hostToResolved){
            Set<Map.Entry<String, String>> entries = hostToResolved.entrySet();
            if (entries.size() == 0) {
                return null;
            }
            List<String[]> l=new ArrayList<>(entries.size());
            for (Map.Entry<String, String> entry : entries) {
                l.add(new String[]{entry.getKey(),entry.getValue()});
            }
            return l;
        }
    }
    public static InetSocketAddress getConnectAddress(Server server){
        return getConnectAddress(server.getListenerAddress());
    }
    public static InetSocketAddress getConnectAddress(InetSocketAddress addr){
        if (!addr.isUnresolved() && addr.getAddress().isAnyLocalAddress()) {
            try {
                addr=new InetSocketAddress(InetAddress.getLocalHost(),addr.getPort());
            }catch (UnknownHostException e){
                addr=createSocketAddrForHost("127.0.0.1",addr.getPort());
            }
        }
        return addr;
    }

    public static SocketInputWrapper

}
