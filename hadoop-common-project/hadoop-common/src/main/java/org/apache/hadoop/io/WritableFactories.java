package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@InterfaceAudience.Public
@InterfaceStability.Stable
/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/23
 **/
public class WritableFactories {
    private static final Map<Class,WritableFactory> CLASS_TO_FACTORY=new ConcurrentHashMap<>();
    private WritableFactories(){}
    public static void setFactory(Class c,WritableFactory factory){
        CLASS_TO_FACTORY.put(c,factory);
    }
    public static WritableFactory getFactory(Class c){
        return CLASS_TO_FACTORY.get(c);
    }
    public static Writable newInstance(Class<? extends Writable> c, Configuration conf){
        WritableFactory factory = WritableFactories.getFactory(c);
        if (factory != null) {
            Writable result = factory.newInstance();
            if (result instanceof Configurable){
                ((Configurable)result).setConf(conf);
            }
            return result;
        }else {
            return ReflectionUtils.newInstance(c,conf);
        }
    }
    public static Writable newInstance(Class<? extends Writable> c){
        return newInstance(c,null);
    }
}
