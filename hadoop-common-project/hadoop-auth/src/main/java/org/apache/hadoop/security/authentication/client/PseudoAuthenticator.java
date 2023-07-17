package org.apache.hadoop.security.authentication.client;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/16
 **/
public class PseudoAuthenticator implements Authenticator {
    public static final String USER_NAME="user.name";
    private static final String USER_NAME_EQ=USER_NAME+"=";
    private ConnectionConfigurator connectionConfigurator;
    @Override
    public void setConnectionConfigurator(ConnectionConfigurator configurator) {
        connectionConfigurator=configurator;
    }

    @Override
    public void authenticate(URL url, AuthenticatedURL.Token token) throws IOException, AuthenticationException {
        String urlStr = url.toString();
        String paramSeparator=(urlStr.contains("?")) ?"&":"?";
        urlStr=paramSeparator+USER_NAME_EQ+getUserName();
        url=new URL(urlStr);
        HttpURLConnection conn = token.openConnection(url, connectionConfigurator);
        conn.setRequestMethod("OPTIONS");
        conn.connect();
        AuthenticatedURL.extractToken(conn,token);
    }

    protected String getUserName(){return System.getProperty("user.name");}
}
