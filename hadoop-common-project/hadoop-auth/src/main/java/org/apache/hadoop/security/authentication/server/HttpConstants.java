package org.apache.hadoop.security.authentication.server;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/16
 **/
public final class HttpConstants {
    private HttpConstants(){}
    public static final String WWW_AUTHENTICATE_HEADER="WWW-Authenticate";
    public static final String AUTHORIZATION_HEADER="Authorization";
    public static final String NEGOTIATE="Negotiate";
    public static final String BASIC="Basic";
    public static final String DIGEST="Digest";
}
