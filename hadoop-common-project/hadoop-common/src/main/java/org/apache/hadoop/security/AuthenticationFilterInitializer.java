package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.http.FilterInitializer;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AuthenticationFilterInitializer  extends FilterInitializer {
    static final String PREFIX="hadoop.http.authentication";
    @Override
    public void initFilter(FilterContainer container, Configuration conf) {
        Map<String ,String > filterConfig=getFilterConfigMap(conf,PREFIX);
        container.addFilter("authentication",
                AuthenticationFilter.class.getName(),
                filterConfig);
    }
    public static Map<String ,String > getFilterConfigMap(Configuration conf,String prefix){
        Map<String ,String > filterConfig=new HashMap<>();
        filterConfig.put(AuthenticationFilter.COOKIE_PATH,"/");
        Map<String, String> propsWithPrefix = conf.getPropsWithPrefix(prefix);
        for (Map.Entry<String, String> entry : propsWithPrefix.entrySet()) {
            filterConfig.put(entry.getKey(),entry.getValue());
        }
        String bindAddress = conf.get(HttpServer2.BIND_ADDRESS);
        String principal = filterConfig.get(KerberosAuthenticationHandler.PRINCIPAL);
        if (principal != null) {
            try {
                principal= SecurityUtil.getServerPrincipal(principal,bindAddress);
            }catch (IOException e){
                throw new RuntimeException("Could not resolve Kerberos principal name: "+e.toString(),e);
            }
            filterConfig.put(KerberosAuthenticationHandler.PRINCIPAL,principal);
        }
        return filterConfig;
    }
}
