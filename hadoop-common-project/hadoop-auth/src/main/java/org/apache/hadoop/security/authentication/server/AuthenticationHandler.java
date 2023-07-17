package org.apache.hadoop.security.authentication.server;

import org.apache.hadoop.security.authentication.client.AuthenticationException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Properties;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/16
 **/
public interface AuthenticationHandler {
    String WWW_AUTHENTICATE=HttpConstants.WWW_AUTHENTICATE_HEADER;
    String getType();
    void init(Properties properties) throws ServletException;
    void destroy();
    boolean managementOperation(AuthenticationToken token,
                                HttpServletRequest request,
                                HttpServletResponse response);
    AuthenticationToken authenticate(HttpServletRequest request,HttpServletResponse response) throws AuthenticationException, IOException;
}
