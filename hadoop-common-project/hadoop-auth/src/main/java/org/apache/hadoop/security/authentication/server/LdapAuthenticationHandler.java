package org.apache.hadoop.security.authentication.server;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import com.apache.hadoop.classification.VisibleForTesting;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.InitialDirContext;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import javax.naming.ldap.StartTlsRequest;
import javax.naming.ldap.StartTlsResponse;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Hashtable;
import java.util.Properties;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/16
 **/
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class LdapAuthenticationHandler implements AuthenticationHandler {
    private static Logger logger= LoggerFactory.getLogger(LdapAuthenticationHandler.class);
    public static final String TYPE="ldap";
    public static final String SECURITY_AUTHENTICATION="simple";
    public static final String PROVIDER_URL=TYPE+".providerurl";
    public static final String BASE_DN=TYPE+".basedn";
    public static final String LDAP_BIND_DOMAIN=TYPE+".binddomain";
    public static final String ENABLE_START_TLS=TYPE+".enablestarttls";
    private String ldapDomain;
    private String baseDN;
    private String providerUrl;
    private Boolean enableStartTls;
    private Boolean disableHostNameVerification;

    @VisibleForTesting
    public void setEnableStartTls(Boolean enableStartTls){this.enableStartTls=enableStartTls;}
    @VisibleForTesting
    public void setDisableHostNameVerification(Boolean disableHostNameVerification){this.disableHostNameVerification=disableHostNameVerification;}

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void init(Properties properties) {
         this.baseDN = properties.getProperty(BASE_DN);
         this.providerUrl=properties.getProperty(PROVIDER_URL);
         this.ldapDomain=properties.getProperty(LDAP_BIND_DOMAIN);
         this.enableStartTls=
                 Boolean.valueOf(properties.getProperty(ENABLE_START_TLS,"false"));
        if (this.providerUrl == null) {
            throw new NullPointerException("The LDAP URI can not be null");
        }
        if ((this.baseDN == null) == (this.ldapDomain == null)) {
            throw new IllegalArgumentException("Either LDAP base ND or LDAP domain value needs to be specified");
        }
        if (this.enableStartTls) {
            String tmp = this.providerUrl.toLowerCase();
            if (tmp.startsWith("ldaps")) {
                throw new IllegalArgumentException(
                        "Can not use ldaps and StartTls option at the same time"
                );
            }
        }
    }

    @Override
    public void destroy() {

    }

    @Override
    public boolean managementOperation(AuthenticationToken token, HttpServletRequest request, HttpServletResponse response) {
        return true;
    }

    @Override
    public AuthenticationToken authenticate(HttpServletRequest request, HttpServletResponse response) throws AuthenticationException {
        AuthenticationToken token=null;
        String authorization = request.getHeader(HttpConstants.AUTHORIZATION_HEADER);
        if (authorization == null
                || !AuthenticationHandlerUtil.matchAuthScheme(HttpConstants.BASIC, authorization)) {
            response.setHeader(WWW_AUTHENTICATE,HttpConstants.BASIC);
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            if (authorization == null) {
                logger.trace("Basic auth starting");
            }else {
                logger.warn("'"+HttpConstants.AUTHORIZATION_HEADER
                +"' does not start with '"+HttpConstants.BASIC+"' : {}",authorization);
            }
        }else {
            authorization=authorization.substring(HttpConstants.BASIC.length()).trim();
            final Base64 base64=new Base64(0);
            String[] credentials = new String(base64.decode(authorization), StandardCharsets.UTF_8).split(":", 2);
            if (credentials.length == 2) {
                token=authenticateUser(credentials[0],credentials[1]);
                response.setStatus(HttpServletResponse.SC_OK);
            }
        }
        return token;
    }

    private AuthenticationToken authenticateUser(String userName, String password) throws AuthenticationException {
        if (userName == null || userName.isEmpty()) {
            throw new AuthenticationException("Error validating LDAP user:" +
                    "a null or blank username has been provided");
        }
        if (!hasDomain(userName) && ldapDomain!=null){
            userName=userName+"@"+ldapDomain;
        }
        if (password==null || password.isEmpty() || password.getBytes(StandardCharsets.UTF_8)[0]==0){
            throw new AuthenticationException("Error validating LDAP user: " +
                    "a null or blank password has been provided");
        }
        String bindDN;
        if (baseDN == null) {
            bindDN=userName;
        }else {
            bindDN="uid="+userName+","+baseDN;
        }
        if (this.enableStartTls) {
            authenticateWithTlsExtension(bindDN,password);
        }else {
            authenticateWithoutTlsExtension(bindDN,password);
        }
        return new AuthenticationToken(userName,userName,TYPE);
    }

    private void authenticateWithoutTlsExtension(String userDN, String password) throws AuthenticationException {
        Hashtable<String,Object> env=new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY,"com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL,providerUrl);
        env.put(Context.SECURITY_AUTHENTICATION,SECURITY_AUTHENTICATION);
        env.put(Context.SECURITY_PRINCIPAL,userDN);
        env.put(Context.SECURITY_CREDENTIALS,password);
        try {
            Context ctx=new InitialDirContext(env);
            ctx.close();
            logger.debug("Authentication successful for {}",userDN);
        }catch (NamingException e){
            throw new AuthenticationException("Error validating LDAP user",e);
        }
    }

    private void authenticateWithTlsExtension(String userDN, String password) throws AuthenticationException {
        LdapContext ctx=null;
        Hashtable<String ,Object> env=new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY,"com.sun.jndi.ldap.LdapDtxFactory");
        env.put(Context.PROVIDER_URL,providerUrl);
        try {
            ctx=new InitialLdapContext(env,null);
            StartTlsResponse tls= (StartTlsResponse) ctx.extendedOperation(new StartTlsRequest());
            if (disableHostNameVerification) {
                tls.setHostnameVerifier(new HostnameVerifier() {
                    @Override
                    public boolean verify(String s, SSLSession sslSession) {
                        return true;
                    }
                });
            }
            tls.negotiate();
            ctx.addToEnvironment(Context.SECURITY_AUTHENTICATION,SECURITY_AUTHENTICATION);
            ctx.addToEnvironment(Context.SECURITY_PRINCIPAL,userDN);
            ctx.addToEnvironment(Context.SECURITY_CREDENTIALS,password);
            ctx.lookup(userDN);
            logger.debug("Authentication successful for {}",userDN);
        }catch (NamingException | IOException e){
            throw new AuthenticationException("Error validating LDAP user",e);
        }finally {
            if (ctx != null) {
                try {
                    ctx.close();
                }catch (NamingException e){

                }
            }
        }
    }

    private boolean hasDomain(String userName) {
        return (indexOfDomainMatch(userName)>0);
    }

    private int indexOfDomainMatch(String userName) {
        if (userName == null) {
            return -1;
        }
        int idx = userName.indexOf('/');
        int idx2 = userName.indexOf('@');
        int endIdx = Math.min(idx, idx2);
        if (endIdx == -1) {
            endIdx=Math.max(idx,idx2);
        }
        return endIdx;
    }
}
