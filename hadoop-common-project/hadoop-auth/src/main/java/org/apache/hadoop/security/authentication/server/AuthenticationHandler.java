package org.apache.hadoop.security.authentication.server;

import com.sun.tools.internal.ws.wsdl.document.http.HTTPConstants;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Properties;

public interface AuthenticationHandler {
    String WWW_AUTHENTICATE=HttpConstants.WWW_AUTHENTICATE_HEADER;
    String getType();
    void init(Properties conf);
    void destroy();
    boolean managementOperation(AuthenticationToken token,
                                HttpServletRequest request,
                                HttpServletResponse response);
