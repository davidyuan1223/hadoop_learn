package org.apache.hadoop.security.authentication.client;

/**
 * @Description: Exception thrown when an authentication error occurs
 * @Author: yuan
 * @Date: 2023/07/16
 **/
public class AuthenticationException extends Exception {
    static final long serialVersionUID=0;

    public AuthenticationException(Throwable cause){super(cause);}

    public AuthenticationException(String msg){super(msg);}

    public AuthenticationException(String  msg,Throwable cause){super(msg,cause);}
}
