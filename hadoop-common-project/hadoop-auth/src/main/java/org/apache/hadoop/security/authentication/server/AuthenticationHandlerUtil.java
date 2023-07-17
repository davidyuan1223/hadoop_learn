package org.apache.hadoop.security.authentication.server;
import java.util.Locale;

import static org.apache.hadoop.security.authentication.server.HttpConstants.*;
/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/16
 **/
public final class AuthenticationHandlerUtil {
    private AuthenticationHandlerUtil(){}
    public static String checkAuthScheme(String scheme){
        if (BASIC.equalsIgnoreCase(scheme)) {
            return BASIC;
        } else if (NEGOTIATE.equalsIgnoreCase(scheme)) {
            return NEGOTIATE;
        } else if (DIGEST.equalsIgnoreCase(scheme)) {
            return DIGEST;
        }
        throw new IllegalArgumentException(String.format(
                "Unsupported HTTP authentication scheme %s . Supported schemes are [%s, %s, %s]",
                scheme,BASIC,NEGOTIATE,DIGEST
        ));
    }

    public static String getAuthenticationHandlerClassName(String authHandler) {
        if (authHandler == null) {
            throw new NullPointerException();
        }
        String handlerName = authHandler.toLowerCase(Locale.ENGLISH);
        String authHandlerClassName=null;
        switch (handlerName){
            case PseudoAuthenticationHandler.TYPE:
                authHandlerClassName=PseudoAuthenticationHandler.class.getName();
                break;
            case KerberosAuthenticationHandler.TYPE:
                authHandlerClassName=KerberosAuthenticationHandler.class.getName();
                break;
            case MultiSchemeAuthenticationHandler.TYPE:
                authHandlerClassName=MultiSchemeAuthenticationHandler.class.getName();
                break;
            default:
                authHandlerClassName=authHandler;
        }
        return authHandlerClassName;
    }

    public static boolean matchAuthScheme(String scheme, String auth) {
        if (scheme == null) {
            throw new NullPointerException();
        }
        scheme=scheme.trim();
        if (auth == null) {
            throw new NullPointerException();
        }
        auth=auth.trim();
        return auth.regionMatches(true,0,scheme,0,scheme.length());
    }
}
