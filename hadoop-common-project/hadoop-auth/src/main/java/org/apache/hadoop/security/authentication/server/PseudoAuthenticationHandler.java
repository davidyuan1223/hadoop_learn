package org.apache.hadoop.security.authentication.server;

import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/16
 **/
public class PseudoAuthenticationHandler implements AuthenticationHandler{
    public static final String TYPE="simple";
    public static final String ANONYMOUS_ALLOWED=TYPE+".anonymous.allowed";
    private static final Charset UTF8_CHARSET= StandardCharsets.UTF_8;
    private static final String PSEUDO_AUTH="PseudoAuth";
    private boolean acceptAnonymous;
    private String type;
    public PseudoAuthenticationHandler(){this(TYPE);}
    public PseudoAuthenticationHandler(String type){this.type=type;}

    @Override
    public String getType() {
        return type;
    }

    @Override
    public void init(Properties properties) {
        acceptAnonymous=Boolean.parseBoolean(properties.getProperty(ANONYMOUS_ALLOWED,"false"));
    }
    protected boolean getAcceptAnonymous(){return acceptAnonymous;}

    @Override
    public void destroy() {

    }

    @Override
    public boolean managementOperation(AuthenticationToken token, HttpServletRequest request, HttpServletResponse response) {
        return true;
    }

    @Override
    public AuthenticationToken authenticate(HttpServletRequest request, HttpServletResponse response) {
        AuthenticationToken token;
        String userName=getUserName(request);
        if (userName == null) {
            if (getAcceptAnonymous()) {
                token=AuthenticationToken.ANONYMOUS;
            }else {
                response.setStatus(HttpServletResponse.SC_FORBIDDEN);
                response.setHeader(WWW_AUTHENTICATE,PSEUDO_AUTH);
                token=null;
            }
        }else {
            token=new AuthenticationToken(userName,userName,getType());
        }
        return token;
    }

    private String getUserName(HttpServletRequest request) {
        String queryString = request.getQueryString();
        if (queryString == null || queryString.length() == 0) {
            return null;
        }
        List<NameValuePair> list = URLEncodedUtils.parse(queryString, UTF8_CHARSET);
        if (list != null) {
            for (NameValuePair nv : list) {
                if (PseudoAuthenticator.USER_NAME.equals(nv.getName())) {
                    return nv.getValue();
                }
            }
        }
        return null;
    }
}
