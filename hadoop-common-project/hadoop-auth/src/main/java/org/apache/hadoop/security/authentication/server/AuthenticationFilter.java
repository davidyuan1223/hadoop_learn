package org.apache.hadoop.security.authentication.server;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.authentication.util.Signer;
import org.apache.hadoop.security.authentication.util.SignerSecretProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.*;
import java.io.IOException;
import java.util.Properties;

/**
 * the authenticationFilter enables protecting web application resources with different authentication
 * mechanisms and signer secret providers.
 * additional authentication mechanisms are supported via the AuthenticationHandler interface
 * this filter delegates to the configured authentication handler for authentication and once it obtains an
 * AuthenticationToken from it,sets a signed HTTP cookie with the token.for client requests that provide the
 * signed HTTP cookie.it verifies the validity of the cookie,extracts the user information and lets the request proceed
 * to the target resource.
 * the rest of the configuration properties are specific to the AuthenticationHandler implementation and the
 * AuthenticationFilter will take all the properties that start with the prefix #PREFIX#,it will remove the prefix
 * from it and it will pass them to the authentication handler for init,properties that do not start with the prefix
 * will not be passed to the authentication handler init
 * the zookeeper implementation has additional configuration properties that must be specified
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class AuthenticationFilter implements Filter {
    private static Logger logger= LoggerFactory.getLogger(AuthenticationFilter.class);
    /**
     * Constant for the property that specifies the configuration
     */
    public static final String CONFIG_PREFIX="config.prefix";
    public static final String AUTH_TYPE="type";
    public static final String SIGNATURE_SECRET="signature.secret";
    public static final String SIGNATURE_SECRET_FILE=SIGNATURE_SECRET+".file";
    public static final String AUTH_TOKEN_MAX_INACTIVE_INTERVAL="token.max-inactive-interval";
    public static final String AUTH_TOKEN_VALIDITY="token.validity";
    public static final String COOKIE_DOMAIN="cookie.domain";
    public static final String COOKIE_PATH="cookie.path";
    public static final String COOKIE_PERSISTENT="cookie.persistent";
    public static final String SIGNER_SECRET_PROVIDER="signer.secret.provider";
    public static final String SIGNER_SECRET_PROVIDER_ATTRIBUTE="signer.secret.provider.object";

    private Properties config;
    private Signer signer;
    private SignerSecretProvider secretProvider;
    private AuthenticationHandler
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {

    }

    @Override
    public void destroy() {

    }
}
