package org.apache.hadoop.security.authentication.server;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.thirdparty.com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.*;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/16
 **/
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MultiSchemeAuthenticationHandler implements CompositeAuthenticationHandler{
    private static Logger logger= LoggerFactory.getLogger(MultiSchemeAuthenticationHandler.class);
    public static final String SCHEMES_PROPERTY="multi-scheme-auth-handler.schemes";
    public static final String AUTH_HANDLER_PROPERTY="multi-scheme-auth-handler.schemes.%s.handler";
    private static final Splitter STR_SPLITTER=Splitter.on(",").trimResults().omitEmptyStrings();
    private final Map<String ,AuthenticationHandler> schemeToAuthHandlerMapping=new HashMap<>();
    private final Collection<String > types=new HashSet<>();
    private final String authType;
    public static final String TYPE="multi-scheme";
    public MultiSchemeAuthenticationHandler(){this(TYPE);}
    public MultiSchemeAuthenticationHandler(String authType){this.authType=authType;}

    @Override
    public Collection<String> getTokenTypes() {
        return types;
    }

    @Override
    public String getType() {
        return authType;
    }

    @Override
    public void init(Properties properties) throws ServletException {
        for (Map.Entry entry : properties.entrySet()) {
            logger.info("{} : {}",entry.getKey(),entry.getValue());
        }
        this.types.clear();
        if (properties.getProperty(SCHEMES_PROPERTY) == null) {
            throw new NullPointerException(SCHEMES_PROPERTY+" system property is not specified");
        }
        String schemesProperty = properties.getProperty(SCHEMES_PROPERTY);
        for (String scheme : STR_SPLITTER.split(schemesProperty)) {
            scheme=AuthenticationHandlerUtil.checkAuthScheme(scheme);
            if (schemeToAuthHandlerMapping.containsKey(scheme)) {
                throw new IllegalArgumentException("Handler is already specified for"
                +scheme+" authentication scheme.");
            }
            String authHandlerPropName = String.format(AUTH_HANDLER_PROPERTY, scheme).toLowerCase();
            String authHandlerName = properties.getProperty(authHandlerPropName);
            if (authHandlerName == null) {
                throw new NullPointerException("No auth handler configured for scheme "+scheme);
            }
            String authHandlerClassName=AuthenticationHandlerUtil
                    .getAuthenticationHandlerClassName(authHandlerName);
            AuthenticationHandler handler=initializeAuthHandler(authHandlerClassName,properties);
            schemeToAuthHandlerMapping.put(scheme,handler);
            types.add(handler.getType());
        }
        logger.info("Successfully initialized MultiSchemeAuthenticationHandler");
    }

    private AuthenticationHandler initializeAuthHandler(String authHandlerClassName, Properties properties) throws ServletException {
        try {
            if (authHandlerClassName == null) {
                throw new NullPointerException();
            }
            logger.debug("Initializing Authentication handler of type "+authHandlerClassName);
            Class<?> klass = Thread.currentThread().getContextClassLoader().loadClass(authHandlerClassName);
            AuthenticationHandler authenticationHandler= (AuthenticationHandler) klass.newInstance();
            authenticationHandler.init(properties);
            logger.info("Successfully initialized Authentication handler of type "+authHandlerClassName);
            return authenticationHandler;
        } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
            logger.error("Failed to initialized authentication handler "+authHandlerClassName,e);
            throw new ServletException(e);
        }
    }

    @Override
    public void destroy() {
        for (AuthenticationHandler handler : schemeToAuthHandlerMapping.values()) {
            handler.destroy();
        }
    }

    @Override
    public boolean managementOperation(AuthenticationToken token, HttpServletRequest request, HttpServletResponse response) {
        return true;
    }

    @Override
    public AuthenticationToken authenticate(HttpServletRequest request, HttpServletResponse response) {
        String authorization = request.getHeader(HttpConstants.AUTHORIZATION_HEADER);
        if (authorization != null) {
            for (Map.Entry<String, AuthenticationHandler> entry : schemeToAuthHandlerMapping.entrySet()) {
                if (AuthenticationHandlerUtil.matchAuthScheme(entry.getKey(),authorization)){
                    AuthenticationToken token = entry.getValue().authenticate(request, response);
                    logger.trace("Token generated with type {}",token.getType());
                    return token;
                }
            }
        }
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        for (String scheme : schemeToAuthHandlerMapping.keySet()) {
            response.addHeader(HttpConstants.WWW_AUTHENTICATE_HEADER,scheme);
        }
        return null;
    }
}
