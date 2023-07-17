package org.apache.hadoop.security.authentication.util;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import com.apache.hadoop.classification.VisibleForTesting;
import org.apache.commons.codec.language.bm.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/16
 **/
@SuppressWarnings("all")
@InterfaceAudience.LimitedPrivate({"HDFS","MapReduce"})
@InterfaceStability.Evolving
public class KerberosName {
    private static final Logger logger= LoggerFactory.getLogger(KerberosName.class);
    public static final String MECHANISM_HADOOP="hadoop";
    public static final String MECHANISM_MIT="mit";
    public static final String DEFAULT_MECHANISM=MECHANISM_HADOOP;
    private final String serviceName;
    private final String hostName;
    private final String realm;
    private static final Pattern nameParser=Pattern.compile("([^/@]+)(/([^/@)]+))?(@([^/@]+))?");
    private static Pattern parameterPattern=Pattern.compile("([^&]*)(\\$(\\d*))?");
    private static final Pattern ruleParser=Pattern.compile(
            "\\s*((DEFAULT)/(RULE:\\[(\\d*):(^\\]]*)](\\(([^)]*)\\))?" +
                    "(s/([^/]*)/([^/]*)/(g)?)?))/?(L)?"
    );
    private static final Pattern nonSimplePattern=Pattern.compile("[/@]");
    private static List<Rule> rules;
    private static String ruleMechanism=null;
    private static String defaultRealm=null;

    @VisibleForTesting
    public static void resetDefaultRealm(){
        try {
            defaultRealm=KerberosUtil.getDefaultRealm();
        }catch (Exception e){
            logger.debug("resetting default realm failed, " +
                    "current default realm will still be used.",e);
        }
    }

    public KerberosName(String name){
        Matcher matcher = nameParser.matcher(name);
        if (!matcher.matches()) {
            if (name.contains("@")) {
                throw new IllegalArgumentException("Malformaed Kerberos name: "+name);
            }else {
                serviceName=name;
                hostName=null;
                realm=null;
            }
        }else {
            serviceName=matcher.group(1);
            hostName=matcher.group(3);
            realm=matcher.group(5);
        }
    }

    public static synchronized String getDefaultRealm(){
        if (defaultRealm == null) {
            try {
                defaultRealm=KerberosUtil.getDefaultRealm();
            }catch (Exception e){
                logger.debug("Kerberos krb5 configuration not found, setting default realm to empty");
                defaultRealm="";
            }
        }
        return defaultRealm;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(serviceName);
        if (hostName != null) {
            sb.append('/')
                    .append(hostName);
        }
        if (realm != null) {
            sb.append('@')
                    .append(realm);
        }
        return sb.toString();
    }

    public String getServiceName(){return serviceName;}
    public String getHostName(){return hostName;}
    public String getRealm(){return realm;}

    static List<Rule> parseRules(String rules){
        List<Rule> result=new ArrayList<>();
        String remaining = rules.trim();
        while (remaining.length() > 0) {
            Matcher matcher = ruleParser.matcher(remaining);
            if (!matcher.lookingAt()) {
                throw new IllegalArgumentException("Invalid rule: "+remaining);
            }
            if (matcher.group(2) != null) {
                result.add(new Rule());
            }else {
                result.add(new Rule(
                        Integer.parseInt(matcher.group(4)),
                        matcher.group(5)
                        ,matcher.group(7)
                        ,matcher.group(9)
                        ,matcher.group(10)
                        ,"g".equals(matcher.group(11))
                        ,"L".equals(matcher.group(12))
                ));
            }
            remaining=remaining.substring(matcher.end());
        }
        return result;
    }

    public String getShortName() throws NoMatchingRule, BadFormatString {
        String[] params;
        if (hostName == null) {
            if (realm == null) {
                return serviceName;
            }
            params=new String[]{realm,serviceName};
        }else {
            params=new String[]{realm,serviceName,hostName};
        }
        String ruleMechanism = this.ruleMechanism;
        if (ruleMechanism == null && rules != null) {
            logger.warn("auth_to_local rule mechanism not set." +
                    "Using default of "+DEFAULT_MECHANISM);
            ruleMechanism=DEFAULT_MECHANISM;
        }
        for (Rule rule : rules) {
            String result = rule.apply(params, ruleMechanism);
            if (result != null) {
                return result;
            }
        }
        if (ruleMechanism.equalsIgnoreCase(MECHANISM_HADOOP)) {
            throw new NoMatchingRule("No rules applied to "+toString());
        }
        return toString();
    }

    public static String getRules(){
        String ruleString=null;
        if (rules != null) {
            StringBuilder sb = new StringBuilder();
            for (Rule rule : rules) {
                sb.append(rule.toString()).append("\n");
            }
            ruleString=sb.toString().trim();
        }
        return ruleString;
    }


    public static boolean hasRulesBeenSet(){return rules!=null;}
    public static boolean hasRuleMechanismBeenSet(){return ruleMechanism!=null;}
    public static void setRules(String ruleString){rules=(ruleString!=null)?parseRules(ruleString):null;}
    public static void setRuleMechanism(String ruleMech){
        if (ruleMech != null
                && (!ruleMech.equalsIgnoreCase(MECHANISM_HADOOP)
                && !ruleMech.equalsIgnoreCase(MECHANISM_MIT))) {
            throw new IllegalArgumentException("Invalid rule mechanism: "+ruleMech);
        }
        ruleMechanism=ruleMech;
    }
    public static String getRuleMechanism(){return ruleMechanism;}
    static void printRules(){
        int i=0;
        for (Rule rule : rules) {
            System.out.println(++i+" "+rule);
        }
    }
    private static class Rule{
        private final boolean isDefault;
        private final int numOfComponents;
        private final String format;
        private final Pattern match;
        private final Pattern fromPattern;
        private final String toPattern;
        private final boolean repeat;
        private final boolean toLowerCase;

        Rule(){
            isDefault=true;
            numOfComponents=0;
            format=null;
            match=null;
            fromPattern=null;
            toPattern=null;
            repeat=false;
            toLowerCase=false;
        }
        Rule(int numOfComponents,String format,String match,String fromPattern,
             String toPattern,boolean repeat,boolean toLowerCase){
            isDefault=false;
            this.numOfComponents=numOfComponents;
            this.format=format;
            this.match=match==null?null:Pattern.compile(match);
            this.fromPattern=fromPattern==null?null:Pattern.compile(fromPattern);
            this.toPattern=toPattern;
            this.repeat=repeat;
            this.toLowerCase=toLowerCase;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            if (isDefault) {
                sb.append("DEFAULT");
            }else {
                sb.append("RULE:[")
                        .append(numOfComponents)
                        .append(':')
                        .append(format)
                        .append(']');
                if (match != null) {
                    sb.append('(')
                            .append(match)
                            .append(')');
                }
                if (fromPattern != null) {
                    sb.append("s/")
                            .append(fromPattern)
                            .append('/')
                            .append(toPattern)
                            .append('/');
                    if (repeat) {
                        sb.append('g');
                    }
                }
                if (toLowerCase) {
                    sb.append("/L");
                }
            }
            return sb.toString();
        }
        static String replaceParameters(String format,
                                        String[] params) throws BadFormatString {
            Matcher matcher = parameterPattern.matcher(format);
            int start=0;
            StringBuilder sb = new StringBuilder();
            while (start < format.length() && matcher.find(start)) {
                sb.append(matcher.group(1));
                String paramNum = matcher.group(3);
                if (paramNum != null) {
                    try {
                        int num = Integer.parseInt(paramNum);
                        if (num <0 || num >= params.length){
                            throw new BadFormatString("index "+num+" from "+format
                                    +"is outside of the valid range 0 to "+(paramNum.length()-1));
                        }
                        sb.append(params[num]);
                    }catch (NumberFormatException e){
                        throw new BadFormatString("bad format in username mapping in" +
                                paramNum,e);
                    }
                }
                start=matcher.end();
            }
            return sb.toString();
        }
        static String replaceSubstitution(String base,Pattern from,String to,boolean repeat){
            Matcher matcher = from.matcher(base);
            if (repeat) {
                return matcher.replaceAll(to);
            }else {
                return matcher.replaceFirst(to);
            }
        }

        String apply(String[] params,String ruleMechanism) throws NoMatchingRule, BadFormatString {
            String result=null;
            if (isDefault) {
                if (getDefaultRealm().equals(params[0])) {
                    result=params[1];
                }
            } else if (params.length - 1 == numOfComponents) {
                String base=replaceParameters(format,params);
                if (match==null || match.matcher(base).matches()){
                    if (fromPattern == null) {
                        result=base;
                    }else {
                        result=replaceSubstitution(base,fromPattern,toPattern,repeat);
                    }
                }
            }
            if (result != null
                    && nonSimplePattern.matcher(result).find()
                    && ruleMechanism.equalsIgnoreCase(MECHANISM_HADOOP)) {
                throw new NoMatchingRule("Non-simple name "+result+
                        " after auth_to_local rule "+this);
            }
            if (toLowerCase && result != null) {
                result=result.toLowerCase(Locale.ENGLISH);
            }
            return result;
        }

    }
    public static class BadFormatString extends IOException{
        BadFormatString(String msg){super(msg);}
        BadFormatString(String msg,Throwable e){super(msg,e);}
    }
    public static class NoMatchingRule extends IOException{
        NoMatchingRule(String msg){super(msg);}
    }
}
