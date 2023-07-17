package org.apache.hadoop.security.authentication.server;

import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.util.AuthToken;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/16
 **/
public class AuthenticationToken extends AuthToken {
    public static final AuthenticationToken ANONYMOUS=new AuthenticationToken();
    private AuthenticationToken(){super();}
    private AuthenticationToken(AuthToken authToken){
        super(authToken.getUserName(),authToken.getName(),authToken.getType());
        setMaxInactives(authToken.getMaxInactives());
        setExpires(authToken.getExpires());
    }
    public AuthenticationToken(String userName,String principal,String type){super(userName,principal,type);}
    public void setMaxInactives(long maxInactives){
        if (this != AuthenticationToken.ANONYMOUS) {
            super.setMaxInactives(maxInactives);
        }
    }
    public void setExpires(long expires){
        if (this != AuthenticationToken.ANONYMOUS) {
            super.setExpires(expires);
        }
    }
    public boolean isExpired(){return super.isExpired();}
    public static AuthenticationToken parse(String tokenStr) throws AuthenticationException {
        return new AuthenticationToken(AuthToken.parse(tokenStr));
    }
}
