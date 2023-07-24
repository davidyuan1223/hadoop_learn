package org.apache.hadoop.security.authentication.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.*;
import java.util.*;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/16
 **/
public class AuthenticatedURL {
    private static final Logger LOG= LoggerFactory.getLogger(AuthenticatedURL.class);
    public static final String AUTH_COOKIE="hadoop.auth";
    private static Class<? extends Authenticator> DEFAULT_AUTHENTICATOR=KerberosAuthenticator.class;
    private Authenticator authenticator;
    private ConnectionConfigurator connectionConfigurator;

    public AuthenticatedURL(){this(null);}
    public AuthenticatedURL(Authenticator authenticator){this(authenticator,null);}
    public AuthenticatedURL(Authenticator authenticator,ConnectionConfigurator connectionConfigurator){
        try {
            this.authenticator=(authenticator!=null)?authenticator:DEFAULT_AUTHENTICATOR.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.connectionConfigurator=connectionConfigurator;
        this.authenticator.setConnectionConfigurator(connectionConfigurator);
    }
    public static void setDefaultAuthenticator(Class<? extends Authenticator> authenticator){
        DEFAULT_AUTHENTICATOR=authenticator;
    }
    public static Class<? extends Authenticator> getDefaultAuthenticator(){
        return DEFAULT_AUTHENTICATOR;
    }
    protected Authenticator getAuthenticator(){
        return authenticator;
    }
    public HttpURLConnection openConnection(URL url,Token token) throws Exception {
        if (url == null) {
            throw new IllegalArgumentException("url cannot be null");
        }
        if (!url.getProtocol().equalsIgnoreCase("http")
                && !url.getProtocol().equalsIgnoreCase("https")) {
            throw new IllegalArgumentException("url must be fore a HTTP or HTTPS resource");
        }
        authenticator.authenticate(url,token);
        return token.openConnection(url,connectionConfigurator);
    }
    public static void injectToken(HttpURLConnection conn,Token token){
        HttpCookie authCookie=token.cookieHandler.getAuthCookie();
        if (authCookie != null) {
            conn.addRequestProperty("Cookie",authCookie.toString());
        }
    }
    public static void extractToken(HttpURLConnection conn,Token token) throws IOException, AuthenticationException {
        int respCode = conn.getResponseCode();
        if (respCode==HttpURLConnection.HTTP_OK
        ||respCode==HttpURLConnection.HTTP_CREATED
        ||respCode==HttpURLConnection.HTTP_ACCEPTED){
            token.cookieHandler.put(null,conn.getHeaderFields());
        } else if (respCode == HttpURLConnection.HTTP_NOT_FOUND) {
            LOG.trace("Setting token value to null ({}),resp={}",token,respCode);
            token.set(null);
            throw new FileNotFoundException(conn.getURL().toString());
        }else {
            LOG.trace("Setting token value to null ({}),resp={}",token,respCode);
            token.set(null);
            throw new AuthenticationException("Authentication failed"+
                    ",URL: "+conn.getURL()+
                    ",status: "+conn.getResponseCode()+
                    ",message: "+conn.getResponseMessage());
        }
    }


    private static class AuthCookieHandler extends CookieHandler{
        private HttpCookie authCookie;
        private Map<String ,List<String>> cookieHeaders= Collections.emptyMap();
        @Override
        public synchronized Map<String, List<String>> get(URI uri, Map<String, List<String>> requestHeaders) throws IOException {
            getAuthCookie();
            return cookieHeaders;
        }

        private synchronized HttpCookie getAuthCookie() {
            if (authCookie != null && authCookie.hasExpired()) {
                setAuthCookie(null);
            }
            return authCookie;
        }

        private synchronized void setAuthCookie(HttpCookie cookie){
            final HttpCookie oldCookie=authCookie;
            authCookie=null;
            cookieHeaders=Collections.emptyMap();
            boolean valid=cookie!=null&&!cookie.getValue().isEmpty()&&!cookie.hasExpired();
            if (valid) {
                long maxAge = cookie.getMaxAge();
                if (maxAge != -1) {
                    cookie.setMaxAge(maxAge*9/10);
                    valid=!cookie.hasExpired();
                }
            }
            if (valid) {
                if (cookie.getVersion() == 0) {
                    String value = cookie.getValue();
                    if (!value.startsWith("\"")) {
                        value="\""+value+"\"";
                        cookie.setValue(value);
                    }
                }
                authCookie=cookie;
                cookieHeaders=new HashMap<>();
                cookieHeaders.put("Cookie", Collections.singletonList(cookie.toString()));
            }
        }

        @Override
        public void put(URI uri, Map<String, List<String>> responseHeaders) throws IOException {
            List<String> headers = responseHeaders.get("Set-Cookie");
            if (headers == null) {
                headers=responseHeaders.get("set-cookie");
            }
            if (headers != null) {
                for (String header : headers) {
                    List<HttpCookie> cookies;
                    try {
                        cookies=HttpCookie.parse(header);
                    }catch (IllegalArgumentException e){
                        LOG.debug("Cannot parse cookie header,header={},reason={}",header,e);
                        continue;
                    }
                    for (HttpCookie cookie : cookies) {
                        if (AUTH_COOKIE.equalsIgnoreCase(cookie.getName())){
                            setAuthCookie(cookie);
                        }
                    }
                }
            }
        }
        private void setAuthCookieValue(String value){
            HttpCookie c=null;
            if (value != null) {
                c=new HttpCookie(AUTH_COOKIE,value);
            }
            setAuthCookie(c);
        }
    }

    public static class Token{
        private final AuthCookieHandler cookieHandler=new AuthCookieHandler();

        public Token(){}
        public Token(String tokenStr){
            if (tokenStr == null) {
                throw new IllegalArgumentException("tokenStr cannot be null");
            }
            set(tokenStr);
        }
        void set(String tokenStr){
            cookieHandler.setAuthCookieValue(tokenStr);
        }
        public boolean isSet(){return cookieHandler.getAuthCookie()!=null;}
        HttpURLConnection openConnection(URL url,
                                         ConnectionConfigurator connectionConfigurator) throws IOException {
            final HttpURLConnection connection;
            synchronized (CookieHandler.class){
                CookieHandler current = CookieHandler.getDefault();
                CookieHandler.setDefault(cookieHandler);
                try {
                    connection= (HttpURLConnection) url.openConnection();
                }finally {
                    CookieHandler.setDefault(current);
                }
            }
            if (connectionConfigurator != null) {
                connectionConfigurator.configure(connection);
            }
            return connection;
        }

        @Override
        public String toString() {
            String value="";
            HttpCookie authCookie = cookieHandler.getAuthCookie();
            if (authCookie != null) {
                value=authCookie.getValue();
                if (value.startsWith("\"")) {
                    value=value.substring(1,value.length()-1);
                }
            }
            return value;
        }
    }
}
