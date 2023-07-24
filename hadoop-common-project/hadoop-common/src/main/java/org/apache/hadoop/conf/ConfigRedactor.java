package org.apache.hadoop.conf;
import org.apache.hadoop.util.StringUtils;

import static org.apache.hadoop.fs.CommonConfigurationKeys.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class ConfigRedactor {
    private static final String REDACTED_TEXT="<redacted>";
    private static final String REDACTED_XML="******";
    private List<Pattern> compiledPatterns;
    public ConfigRedactor(Configuration conf){
        String sensitiveRegexList = conf.get(HADOOP_SECURITY_SENSITIVE_CONFIG_KEYS,
                HADOOP_SECURITY_SENSITIVE_CONFIG_KEYS_DEFAULT);
        List<String> sensitiveRegexes=Arrays.asList(StringUtils.getTrimmedStrings(sensitiveRegexList));
        compiledPatterns=new ArrayList<>();
        for (String regex : sensitiveRegexes) {
            Pattern p = Pattern.compile(regex);
            compiledPatterns.add(p);
        }
    }
    public String redact(String key,String value){
        if (configIsSensitive(key)) {
            return REDACTED_TEXT;
        }
        return value;
    }
    private boolean configIsSensitive(String key){
        for (Pattern regex : compiledPatterns) {
            if (regex.matcher(key).find()) {
                return true;
            }
        }
        return false;
    }
    public String redactXml(String key,String value){
        if (configIsSensitive(key)) {
            return REDACTED_XML;
        }
        return value;
    }
}
