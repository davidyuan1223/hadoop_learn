package org.apache.hadoop.security.authentication.server;

import org.apache.hadoop.security.authentication.util.AuthToken;

public class AuthenticationToken extends AuthToken {
    public static final AuthenticationToken ANONYMOUS=new AuthenticationToken();
    public AuthenticationToken(){super();}
    public AuthenticationToken(AuthToken token){
        super(token.getUserName(),token.getName(),token.getType());
        setMaxInactives(token.getMaxInactinves());
        setExpires(token.getExpires());
    }

}
