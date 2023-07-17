package org.apache.hadoop.security.authentication.server;

import com.apache.hadoop.classification.VisibleForTesting;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.ietf.jgss.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KeyTab;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.awt.print.PrinterAbortException;
import java.io.File;
import java.io.IOException;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/16
 **/
public class KerberosAuthenticationHandler implements AuthenticationHandler{
    public static final Logger LOG= LoggerFactory.getLogger(KerberosAuthenticationHandler.class);
    public static final String TYPE="kerberos";
    public static final String PRINCIPAL=TYPE+".principal";
    public static final String KEYTAB=TYPE+".keytab";
    public static final String NAME_RULES=TYPE+".name.rules";
    public static final String RULE_MECHANISM=TYPE+"name.rules.mechanism";
    @VisibleForTesting
    static final String ENDPOINT_WHITELIST=TYPE+".endpoint.whitelist";
    private static final Pattern ENDPOINT_PATTERN=Pattern.compile("^/[\\w]+");
    private String type;
    private String keytab;
    private GSSManager gssManager;
    private Subject serverSubject=new Subject();
    private final Collection<String > whitelist=new HashSet<>();

    public KerberosAuthenticationHandler(){this(TYPE);}
    public KerberosAuthenticationHandler(String type){this.type=type;}

    @Override
    public String getType() {
        return type;
    }

    @Override
    public void init(Properties properties) throws ServletException {
        try {
            String principal = properties.getProperty(PRINCIPAL);
            if (principal == null || principal.trim().length() == 0) {
                throw new ServletException("Principal not defined in configuration");
            }
            keytab=properties.getProperty(KEYTAB,keytab);
            if (keytab == null || keytab.trim().length() == 0) {
                throw new ServletException("Keytab not defined in configuration");
            }
            File keytabFile = new File(keytab);
            if (!keytabFile.exists()) {
                throw new ServletException("Keytab does not exist: "+keytab);
            }
            final String[] spnegoPrincipals;
            if (principal.equalsIgnoreCase("*")) {
                spnegoPrincipals= KerberosUtil.getPrincipalNames(
                        keytab,Pattern.compile("HTTP/.*")
                );
                if (spnegoPrincipals.length == 0) {
                    throw new ServletException("Principle do not exist in the keytab");
                }
            }else {
                spnegoPrincipals=new String[]{principal};
            }
            KeyTab keytabInstance = KeyTab.getInstance(keytabFile);
            serverSubject.getPrivateCredentials().add(keytabInstance);
            for (String spnegoPrincipal : spnegoPrincipals) {
                Principal krbPrincipal = new KerberosPrincipal(spnegoPrincipal);
                LOG.info("Using keytab {}, for principal {}",keytab,krbPrincipal);
                serverSubject.getPrincipals().add(krbPrincipal);
            }
            String nameRules = properties.getProperty(NAME_RULES, null);
            if (nameRules != null) {
                KerberosName.setRules(nameRules);
            }
            String ruleMechanism = properties.getProperty(RULE_MECHANISM, null);
            if (ruleMechanism != null) {
                KerberosName.setRuleMechanism(ruleMechanism);
            }
            final String whitelistStr=properties.getProperty(ENDPOINT_WHITELIST,null);
            if (whitelistStr != null) {
                final String[] strs=whitelistStr.trim().split("\\s*[,\n]\\s*");
                for (String str : strs) {
                    if (str.isEmpty()) {
                        continue;
                    }
                    if (ENDPOINT_PATTERN.matcher(str).matches()) {
                        whitelist.add(str);
                    }else {
                        throw new ServletException(
                                "The element of the whitelist: "+str+
                                        "must start with '/'" +
                                        "and must not contains special characters afterwards"
                        );
                    }
                }
                try {
                    gssManager=Subject.doAs(serverSubject, new PrivilegedExceptionAction<GSSManager>() {
                        @Override
                        public GSSManager run() throws Exception {
                            return GSSManager.getInstance();
                        }
                    });
                }catch (PrivilegedActionException e){
                    throw e.getException();
                }
            }
        }catch (Exception e){
            throw new ServletException(e);
        }
    }

    @Override
    public void destroy() {
        keytab=null;
        serverSubject=null;
    }

    protected Set<KerberosPrincipal> getPrincipals(){
        return serverSubject.getPrincipals(KerberosPrincipal.class);
    }
    protected String getKeytab(){return keytab;}
    @Override
    public boolean managementOperation(AuthenticationToken token, HttpServletRequest request, HttpServletResponse response) {
        return true;
    }

    @Override
    public AuthenticationToken authenticate(HttpServletRequest request, HttpServletResponse response)throws IOException,AuthenticationException {
        final String path = request.getServletPath();
        for (final String endpoint : whitelist) {
            if (endpoint.equals(path)) {
                return AuthenticationToken.ANONYMOUS;
            }
        }
        AuthenticationToken token=null;
        String authorization = request.getHeader(KerberosAuthenticator.AUTHORIZATION);
        if (authorization == null
                || !authorization.startsWith(KerberosAuthenticator.NEGOTIATE)) {
            response.setHeader(WWW_AUTHENTICATE,KerberosAuthenticator.NEGOTIATE);
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            if (authorization == null) {
                LOG.trace("SPNEGO starting for url: {}",request.getRequestURI());
            }else {
                LOG.warn("'"+KerberosAuthenticator.AUTHORIZATION+
                        "' does not start with '" +
                        KerberosAuthenticator.NEGOTIATE+
                        "' : {}",authorization);
            }
        }else {
            authorization=authorization.substring(
                    KerberosAuthenticator.NEGOTIATE.length()
            ).trim();
            final Base64 base64=new Base64(0);
            final byte[] clientToken=base64.decode(authorization);
            try {
                final String serverPrincipal=KerberosUtil.getTokenServerName(clientToken);
                if (!serverPrincipal.startsWith("HTTP/")) {
                    throw new IllegalArgumentException(
                            "Invalid server principal "+serverPrincipal
                            +"decoded from the client request"
                    );
                }
                token=Subject.doAs(serverSubject, new PrivilegedExceptionAction<AuthenticationToken>() {
                    @Override
                    public AuthenticationToken run() throws Exception {
                        return runWithPrincipal(serverPrincipal,clientToken,base64,response);
                    }
                });
            }catch (PrivilegedActionException e){
                if (e.getException() instanceof IOException) {
                    throw (IOException)e.getException();
                }else {
                    throw new AuthenticationException(e.getException());
                }
            }catch (Exception e){
                throw new AuthenticationException(e);
            }
        }
        return token;
    }

    private AuthenticationToken runWithPrincipal(String serverPrincipal, byte[] clientToken, Base64 base64, HttpServletResponse response) throws IOException, GSSException {
        GSSContext gssContext=null;
        GSSCredential gssCreds=null;
        AuthenticationToken token=null;
        try {
            LOG.trace("SPNEGO initiated with server principal [{}]",serverPrincipal);
            gssCreds=this.gssManager.createCredential(
                    this.gssManager.createName(serverPrincipal,
                            KerberosUtil.NT_GSS_KRB5_PRINCIPAL_OID),
                    GSSCredential.INDEFINITE_LIFETIME,
                    new Oid[]{
                            KerberosUtil.GSS_SPNEGO_MECH_OID,
                            KerberosUtil.GSS_KRB5_MECH_OID
                    },
                    GSSCredential.ACCEPT_ONLY
            );
            gssContext=this.gssManager.createContext(gssCreds);
            byte[] serverToken = gssContext.acceptSecContext(clientToken, 0, clientToken.length);
            if (serverToken != null && serverToken.length > 0) {
                String authenticate = base64.encodeToString(serverToken);
                response.setHeader(KerberosAuthenticator.WWW_AUTHENTICATE,
                        KerberosAuthenticator.NEGOTIATE+" "+authenticate);
            }
            if (!gssContext.isEstablished()) {
                response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                LOG.trace("SPNEGO is progress");
            }else {
                String clientPrincipal = gssContext.getSrcName().toString();
                KerberosName kerberosName = new KerberosName(clientPrincipal);
                String userName = kerberosName.getShortName();
                token=new AuthenticationToken(userName,clientPrincipal,getType());
                response.setStatus(HttpServletResponse.SC_OK);
                LOG.trace("SPNEGO completetd for client principal [{}]",clientPrincipal);
            }
        } finally {
            if (gssContext != null) {
                gssContext.dispose();
            }
            if (gssCreds != null) {
                gssCreds.dispose();
            }
        }
        return token;
    }
}
