package org.apache.hadoop.crypto.key;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class KeyProviderFactory {
    public static final String KEY_PROVIDER_PATH=
            CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH;
    public abstract KeyProvider createProvider(URI providerName, Configuration conf)throws IOException;
    private static final ServiceLoader<KeyProviderFactory> serviceLoader=
            ServiceLoader.load(KeyProviderFactory.class,KeyProviderFactory.class.getClassLoader());
    static {
        Iterator<KeyProviderFactory> iterServices = serviceLoader.iterator();
        while (iterServices.hasNext()) {
            iterServices.next();
        }
    }

    public static List<KeyProvider> getProviders(Configuration conf)throws IOException{
        List<KeyProvider> result=new ArrayList<>();
        for (String path : conf.getStringCollection(KEY_PROVIDER_PATH)) {
            try {
                URI uri=new URI(path);
                KeyProvider kp=get(uri,conf);
                if (kp != null) {
                    result.add(kp);
                }else {
                    throw new IOException("No KeyProviderFactory for "+uri+" in "+KEY_PROVIDER_PATH);
                }
            }catch (URISyntaxException e){
                throw new IOException("Bad configuration of "+KEY_PROVIDER_PATH+" at "+path,e);
            }
        }
        return result;
    }

    public static KeyProvider get(URI uri,Configuration conf)throws IOException{
        KeyProvider kp=null;
        for (KeyProviderFactory factory : serviceLoader) {
            kp=factory.createProvider(uri,conf);
            if (kp != null) {
                break;
            }
        }
        return kp;
    }
}
