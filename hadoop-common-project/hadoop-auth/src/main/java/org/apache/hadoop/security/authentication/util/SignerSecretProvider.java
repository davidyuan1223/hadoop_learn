package org.apache.hadoop.security.authentication.util;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import javax.servlet.ServletContext;
import java.util.Properties;

/**
 * The SignerSecretProvider is an abstract way to provide a secret to be used by
 * the Signer so that we can have different implementations that potentially do more
 * complicated things in the backend.
 * See the RolloverSignerSecretProvider class for an implementation that supports
 * rolling over the secret at a regular interval
 */
@InterfaceStability.Unstable
@InterfaceAudience.Private
public abstract class SignerSecretProvider {
    /**
     * initialize the SignerSecretProvider
     * @param conf - configuration properties
     * @param servletContext - servlet context
     * @param tokenValidity - the amount of time a token is valid for
     * @throws Exception - throws if an error occurred
     */
    public abstract void init(Properties conf, ServletContext servletContext,long tokenValidity)throws Exception;

    /**
     * will be called on shutdown;
     * subclass should perform any cleanup here
     */
    public void destroy(){}

    /**
     * returns the current secret to be used by the Signer for signing new cookies.
     * This should never return null,Callers should be careful not to modify the returned value
     * @return the current secret
     */
    public abstract byte[] getCurrentSecret();

    /**
     * returns all secret that a cookie could have been signed with and are still valid.
     * this should include secret returned by getCurrentSecret
     * Callers should be careful not to modify the returned value
     * @return the secrets
     */
    public abstract byte[][] getAllSecret();
}
