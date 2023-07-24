package org.apache.hadoop.conf;

import com.apache.hadoop.classification.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/22
 **/
@InterfaceAudience.Private
public class ConfigurationWithLogging extends Configuration{
    private static final Logger logger= LoggerFactory.getLogger(ConfigurationWithLogging.class);
    private final Logger log;
    private final ConfigRedactor redactor;

    public ConfigurationWithLogging(Configuration conf){
        super(conf);
        log=logger;
        redactor=new ConfigRedactor(conf);
    }

    @Override
    public String get(String name) {
        String value = super.get(name);
        log.info("Got {} = '{}'",name,redactor.redact(name,value));
        return value;
    }

    @Override
    public String get(String name, String defaultValue) {
        String value = super.get(name, defaultValue);
        log.info("Got {} = '{}' (default '{}'",name,redactor.redact(name,value),
                redactor.redact(name,defaultValue));
        return value;
    }
    @Override
    public boolean getBoolean(String name, boolean defaultValue) {
        boolean value = super.getBoolean(name, defaultValue);
        log.info("Got {} = '{}' (default '{}')", name, value, defaultValue);
        return value;
    }

    /**
     * See {@link Configuration#getFloat(String, float)}.
     */
    @Override
    public float getFloat(String name, float defaultValue) {
        float value = super.getFloat(name, defaultValue);
        log.info("Got {} = '{}' (default '{}')", name, value, defaultValue);
        return value;
    }

    /**
     * See {@link Configuration#getInt(String, int)}.
     */
    @Override
    public int getInt(String name, int defaultValue) {
        int value = super.getInt(name, defaultValue);
        log.info("Got {} = '{}' (default '{}')", name, value, defaultValue);
        return value;
    }

    /**
     * See {@link Configuration#getLong(String, long)}.
     */
    @Override
    public long getLong(String name, long defaultValue) {
        long value = super.getLong(name, defaultValue);
        log.info("Got {} = '{}' (default '{}')", name, value, defaultValue);
        return value;
    }

    /**
     * See {@link Configuration#set(String, String, String)}.
     */
    @Override
    public void set(String name, String value, String source) {
        log.info("Set {} to '{}'{}", name, redactor.redact(name, value),
                source == null ? "" : " from " + source);
        super.set(name, value, source);
    }
}
