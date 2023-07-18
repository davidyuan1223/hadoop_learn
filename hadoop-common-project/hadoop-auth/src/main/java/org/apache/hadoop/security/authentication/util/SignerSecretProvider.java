package org.apache.hadoop.security.authentication.util;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import javax.servlet.ServletContext;
import java.util.Properties;

@InterfaceStability.Unstable
@InterfaceAudience.Private
public abstract class SignerSecretProvider {
    public abstract void init(Properties config, ServletContext servletContext,long tokenValidity)throws Exception;

    public void destroy(){}

    public abstract byte[] getCurrentSecret();

    public abstract byte[][] getAllSecrets();
}
