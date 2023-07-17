package org.apache.hadoop.security.authentication.client;

import static org.apache.hadoop.util.PlatformName.IBM_JAVA;

import com.apache.hadoop.classification.VisibleForTesting;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.security.authentication.server.HttpConstants;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.hadoop.security.authentication.util.AuthToken;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/16
 **/
public class KerberosAuthenticator implements Authenticator {
    private static Logger LOG= LoggerFactory.getLogger(KerberosAuthenticator.class);
    public static final String WWW_AUTHENTICATE= HttpConstants.WWW_AUTHENTICATE_HEADER;
    public static final String AUTHORIZATION=HttpConstants.AUTHORIZATION_HEADER;
    public static final String NEGOTIATE=HttpConstants.NEGOTIATE;
    private static final String AUTH_HTTP_METHOD="OPTIONS";
    private URL url;
    private Base64 base64;
    private ConnectionConfigurator connectionConfigurator;
    @Override
    public void setConnectionConfigurator(ConnectionConfigurator configurator) {
        connectionConfigurator=configurator;
    }

    @Override
    public void authenticate(URL url, AuthenticatedURL.Token token) throws Exception {
        if (!token.isSet()) {
            this.url=url;
            base64=new Base64(0);
            HttpURLConnection conn=null;
            try {
                conn=token.openConnection(url,connectionConfigurator);
                conn.setRequestMethod(AUTH_HTTP_METHOD);
                conn.connect();

                boolean needFallback=false;
                if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
                    LOG.debug("JDK performed authentication on our behalf");
                    AuthenticatedURL.extractToken(conn,token);
                    if (isTokenKerberos(token)) {
                        return;
                    }
                    needFallback=true;
                }
                if (!needFallback && isNegotiate(conn)) {
                    LOG.debug("Performing our won SPNEGO sequence.");
                    doSpnegoSequence(token);
                }else {
                    LOG.debug("Using fallback authenticator sequence.");
                    Authenticator auth=getFallbackAuthenticator();
                    auth.setConnectionConfigurator(connectionConfigurator);
                    auth.authenticate(url,token);
                }
            }catch (IOException | AuthenticationException e){
                throw wrapExceptionWithMessage(e,"Error while authenticating with endpoint: "+url);
            }finally {
                if (conn != null) {
                    conn.disconnect();
                }
            }
        }
    }

    private void doSpnegoSequence(AuthenticatedURL.Token token) throws IOException, AuthenticationException {
        try {
            AccessControlContext context = AccessController.getContext();
            Subject subject = Subject.getSubject(context);
            if (subject == null
                    || (!KerberosUtil.hasKerberosKeyTab(subject)
                    && !KerberosUtil.hasKerberosTicket(subject))) {
                LOG.debug("No subject in context, logging in");
                subject = new Subject();
                LoginContext login = new LoginContext("", subject,
                        null, new KerberosConfiguration());
                login.login();
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Using subject: " + subject);
            }
            Subject.doAs(subject, new PrivilegedExceptionAction<Void>() {

                @Override
                public Void run() throws Exception {
                    GSSContext gssContext = null;
                    try {
                        GSSManager gssManager = GSSManager.getInstance();
                        String servicePrincipal = KerberosUtil.getServicePrincipal("HTTP",
                                KerberosAuthenticator.this.url.getHost());
                        Oid oid = KerberosUtil.NT_GSS_KRB5_PRINCIPAL_OID;
                        GSSName serviceName = gssManager.createName(servicePrincipal,
                                oid);
                        oid = KerberosUtil.GSS_KRB5_MECH_OID;
                        gssContext = gssManager.createContext(serviceName, oid, null,
                                GSSContext.DEFAULT_LIFETIME);
                        gssContext.requestCredDeleg(true);
                        gssContext.requestMutualAuth(true);

                        byte[] inToken = new byte[0];
                        byte[] outToken;
                        boolean established = false;

                        // Loop while the context is still not established
                        while (!established) {
                            HttpURLConnection conn =
                                    token.openConnection(url, connectionConfigurator);
                            outToken = gssContext.initSecContext(inToken, 0, inToken.length);
                            if (outToken != null) {
                                sendToken(conn, outToken);
                            }

                            if (!gssContext.isEstablished()) {
                                inToken = readToken(conn);
                            } else {
                                established = true;
                            }
                        }
                    } finally {
                        if (gssContext != null) {
                            gssContext.dispose();
                            gssContext = null;
                        }
                    }
                    return null;
                }
            });
        } catch (PrivilegedActionException ex) {
            if (ex.getException() instanceof IOException) {
                throw (IOException) ex.getException();
            } else {
                throw new AuthenticationException(ex.getException());
            }
        } catch (LoginException ex) {
            throw new AuthenticationException(ex);
        }
    }
    private void sendToken(HttpURLConnection conn, byte[] outToken)
            throws IOException {
        String token = base64.encodeToString(outToken);
        conn.setRequestMethod(AUTH_HTTP_METHOD);
        conn.setRequestProperty(AUTHORIZATION, NEGOTIATE + " " + token);
        conn.connect();
    }
    private byte[] readToken(HttpURLConnection conn)
            throws IOException, AuthenticationException {
        int status = conn.getResponseCode();
        if (status == HttpURLConnection.HTTP_OK || status == HttpURLConnection.HTTP_UNAUTHORIZED) {
            String authHeader = conn.getHeaderField(WWW_AUTHENTICATE);
            if (authHeader == null) {
                authHeader = conn.getHeaderField(WWW_AUTHENTICATE.toLowerCase());
            }
            if (authHeader == null || !authHeader.trim().startsWith(NEGOTIATE)) {
                throw new AuthenticationException("Invalid SPNEGO sequence, '" + WWW_AUTHENTICATE +
                        "' header incorrect: " + authHeader);
            }
            String negotiation = authHeader.trim().substring((NEGOTIATE + " ").length()).trim();
            return base64.decode(negotiation);
        }
        throw new AuthenticationException("Invalid SPNEGO sequence, status code: " + status);
    }
    private Authenticator getFallbackAuthenticator() {
        Authenticator auth = new PseudoAuthenticator();
        if (connectionConfigurator != null) {
            auth.setConnectionConfigurator(connectionConfigurator);
        }
        return auth;
    }

    private boolean isNegotiate(HttpURLConnection conn) throws IOException {
        boolean negotiate=false;
        if (conn.getResponseCode()==HttpURLConnection.HTTP_UNAUTHORIZED){
            String authHeader = conn.getHeaderField(WWW_AUTHENTICATE);
            if (authHeader == null) {
                authHeader=conn.getHeaderField(WWW_AUTHENTICATE.toLowerCase());
            }
            negotiate=authHeader!=null&&authHeader.trim().startsWith(NEGOTIATE);
        }
        return negotiate;
    }

    private boolean isTokenKerberos(AuthenticatedURL.Token token) throws AuthenticationException {
        if (token.isSet()) {
            AuthToken aToken=AuthToken.parse(token.toString());
            if (aToken.getType().equals("kerberos")||
            aToken.getType().equals("kerberos-dt")){
                return true;
            }
        }
        return false;
    }

    @VisibleForTesting
    static <T extends Exception> T wrapExceptionWithMessage(T exception,String msg){
        Class<? extends Throwable> exceptionClass = exception.getClass();
        try {
            Constructor<? extends Throwable> constructor = exceptionClass.getConstructor(String.class);
            Throwable t = constructor.newInstance(msg);
            return (T)(t.initCause(exception));
        }catch (Throwable e){
            LOG.debug("Unable to wrap exception of type {}, it has "
                    + "no (String) constructor.", exceptionClass, e);
            return exception;
        }
    }
    private static class KerberosConfiguration extends Configuration{
        public static final String OS_LOGIN_MODULE_NAME;
        private static final boolean windows=System.getProperty("os.name").startsWith("Windows");
        private static final boolean is64Bit=System.getProperty("os.arch").contains("64");
        private static final boolean aix=System.getProperty("os.name").equals("AIX");
        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
            return new AppConfigurationEntry[0];
        }
        private static String getOsLoginModuleName(){
            if (IBM_JAVA){
                if (windows) {
                    return is64Bit ? "com.ibm.security.auth.module.Win64LoginModule"
                            : "com.ibm.security.auth.module.NTLoginModule";
                } else if (aix) {
                    return is64Bit ? "com.ibm.security.auth.module.AIX64LoginModule"
                            : "com.ibm.security.auth.module.AIXLoginModule";
                } else {
                    return "com.ibm.security.auth.module.LinuxLoginModule";
                }
            } else {
                return windows ? "com.sun.security.auth.module.NTLoginModule"
                        : "com.sun.security.auth.module.UnixLoginModule";
            }
        }
        static {
            OS_LOGIN_MODULE_NAME=getOsLoginModuleName();
        }
        private static final AppConfigurationEntry OS_SPECIFIC_LOGIN=
                new AppConfigurationEntry(OS_LOGIN_MODULE_NAME,
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                        new HashMap<String ,String >());
        private static final Map<String ,String > USER_KERBEROS_OPTIONS=new HashMap<>();
        private static final AppConfigurationEntry USER_KERBEROS_LOGIN=
                new AppConfigurationEntry(KerberosUtil.getKrb5LoginModuleName(),
                        AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL,
                        USER_KERBEROS_OPTIONS);
        private static final AppConfigurationEntry[] USER_KEREROS_CONF=new
                AppConfigurationEntry[]{OS_SPECIFIC_LOGIN,USER_KERBEROS_LOGIN};
        static {
            String ticketCache = System.getenv("KERB5CCNAME");
            if (IBM_JAVA) {
                USER_KERBEROS_OPTIONS.put("useDefaultCcache","true");
            }else {
                USER_KERBEROS_OPTIONS.put("doNotPrompt","true");
                USER_KERBEROS_OPTIONS.put("useTicketCache","true");
            }
            if (ticketCache != null) {
                if (IBM_JAVA) {
                    System.setProperty("KRB5CCNAME",ticketCache);
                }else {
                    USER_KERBEROS_OPTIONS.put("ticketCahe",ticketCache);
                }
            }
            USER_KERBEROS_OPTIONS.put("renewTGT","true");
        }
    }
}
