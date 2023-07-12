package org.apache.hadoop.security.authentication.server;

public final class HttpConstants {
    private HttpConstants(){}
    public static final String WWW_AUTHENTICATE_HEADER="WWW-Authenticate";
    public static final String AUTHORIZATION_HEADER="Authorization";
    public static final String NEGOTIATE="Negotiate";
    public static final String BASIC="basic";
    public static final String DIGEST="Digest";
}
