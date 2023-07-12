package org.apache.hadoop.security.authentication.util;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import javax.servlet.ServletContext;
import java.util.Properties;
@InterfaceStability.Unstable
@InterfaceAudience.Private
public class FileSignerSecretProvider  extends SignerSecretProvider{
    private byte[] secret;
    private byte[][] secrets;
    public FileSignerSecretProvider(){}
    @Override
    public void init(Properties conf, ServletContext servletContext, long tokenValidity) throws Exception {
        conf.getProperty(Authen)
    }

    @Override
    public byte[] getCurrentSecret() {
        return new byte[0];
    }

    @Override
    public byte[][] getAllSecret() {
        return new byte[0][];
    }
}
