package org.apache.hadoop.security.authentication.util;

import org.apache.hadoop.security.authentication.client.AuthenticationException;

import java.security.Principal;
import java.util.*;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/16
 **/
public class AuthToken implements Principal {
    private static final String ATTR_SEPARATOR="&";
    private static final String USER_NAME="u";
    private static final String PRINCIPAL="p";
    private static final String MAX_INACTIVES="i";
    private static final String TYPE="t";
    private static final String EXPIRES="e";
    private static final Set<String > ATTRIBUTES=new HashSet<>(Arrays.asList(USER_NAME,PRINCIPAL,EXPIRES,TYPE));
    private String userName;
    private String principal;
    private String type;
    private long maxInactives;
    private long expires;
    private String tokenStr;
    private static final String ILLEGAL_ARG_MSG=" is NULL, empty or contains a '"+ATTR_SEPARATOR+"'";

    protected AuthToken(){
        userName=null;
        principal=null;
        type=null;
        maxInactives=-1;
        expires=-1;
        tokenStr="ANONYMOUS";
        generateToken();
    }



    public AuthToken(String userName,String principal,String type){
        checkForIllegalArgument(userName,"userName");
        checkForIllegalArgument(principal,"principal");
        checkForIllegalArgument(type,"type");
        this.userName=userName;
        this.principal=principal;
        this.type=type;
        this.maxInactives=-1;
        this.expires=-1;
    }

    protected void checkForIllegalArgument(String value, String name) {
        if (value == null || value.length() == 0 || value.contains(ATTR_SEPARATOR)) {
            throw new IllegalArgumentException(name+ILLEGAL_ARG_MSG);
        }
    }

    public void setMaxInactives(long interval){this.maxInactives=interval;}
    public void setExpires(long expires){
        this.expires=expires;
        generateToken();
    }
    public String getUserName(){return userName;}
    @Override
    public String getName() {
        return principal;
    }
    public String getType(){return type;}
    public long getMaxInactives(){return maxInactives;}
    public long getExpires(){return expires;}

    @Override
    public String toString() {
        return tokenStr;
    }

    public boolean isExpired(){
        return (getMaxInactives()!=-1
        &&System.currentTimeMillis()>getMaxInactives())
                || (getExpires()!=-1&&
                System.currentTimeMillis()>getExpires());
    }

    private void generateToken() {
        StringBuilder sb = new StringBuilder();
        sb.append(USER_NAME)
                .append("=")
                .append(getUserName())
                .append(ATTR_SEPARATOR);
        sb.append(PRINCIPAL)
                .append("=")
                .append(getName())
                .append(ATTR_SEPARATOR);
        sb.append(TYPE)
                .append("=")
                .append(getType())
                .append(ATTR_SEPARATOR);
        if (getMaxInactives() != -1) {
            sb.append(MAX_INACTIVES)
                    .append("=")
                    .append(getMaxInactives())
                    .append(ATTR_SEPARATOR);
        }
        sb.append(EXPIRES)
                .append("=")
                .append(getExpires());
        tokenStr=sb.toString();
    }

    public static AuthToken parse(String tokenStr) throws AuthenticationException {
        if (tokenStr.length() >= 2) {
            if (tokenStr.charAt(0)=='\"'&&tokenStr.charAt(tokenStr.length()-1)=='\"'){
                tokenStr=tokenStr.substring(1,tokenStr.length()-1);
            }
        }
        Map<String ,String > map=split(tokenStr);
        map.remove("s");
        if (!map.keySet().containsAll(ATTRIBUTES)) {
            throw new AuthenticationException("Invalid token string,missing attributes");
        }
        long expires = Long.parseLong(map.get(EXPIRES));
        AuthToken token = new AuthToken(map.get(USER_NAME), map.get(PRINCIPAL), map.get(TYPE));
        if (map.containsKey(MAX_INACTIVES)) {
            long maxInactives = Long.parseLong(map.get(MAX_INACTIVES));
            token.setMaxInactives(maxInactives);
        }
        token.setExpires(expires);
        return token;
    }

    private static Map<String, String> split(String tokenStr) throws AuthenticationException {
        Map<String ,String > map=new HashMap<>();
        StringTokenizer st = new StringTokenizer(tokenStr, ATTR_SEPARATOR);
        while (st.hasMoreTokens()) {
            String part = st.nextToken();
            int separator = part.indexOf("=");
            if (separator == -1) {
                throw new AuthenticationException("Invalid authentication token");
            }
            String key = part.substring(0, separator);
            String value = part.substring(separator + 1);
            map.put(key,value);
        }
        return map;
    }
}
