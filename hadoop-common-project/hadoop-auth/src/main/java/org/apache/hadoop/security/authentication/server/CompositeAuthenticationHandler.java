package org.apache.hadoop.security.authentication.server;

import java.util.Collection;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/16
 **/
public interface CompositeAuthenticationHandler extends AuthenticationHandler{
    Collection<String > getTokenTypes();
}
