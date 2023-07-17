package org.apache.hadoop.security.authentication.client;

import java.io.IOException;
import java.net.URL;

/**
 * @Description: Interface for client authentication mechanisms
 * implementations are use-once instances, they don't need to be thread safe
 * @Author: yuan
 * @Date: 2023/07/16
 **/
public interface Authenticator {

    public void setConnectionConfigurator(ConnectionConfigurator configurator);

    public void authenticate(URL url,AuthenticatedURL.Token token) throws IOException,AuthenticationException, Exception;
}
