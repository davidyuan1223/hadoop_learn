package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;

public class AnnotatedSecurityInfo  extends SecurityInfo{

    @Override
    public KerberosInfo getKerberosInfo(Class<?> protocol, Configuration conf) {
        return protocol.getAnnotation(KerberosInfo.class);
    }

    @Override
    public TokenInfo getTokenInfo(Class<?> protocol, Configuration conf) {
        return protocol.getAnnotation(TokenInfo.class);
    }
}
