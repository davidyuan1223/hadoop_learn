package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import java.util.concurrent.ConcurrentHashMap;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class WritableComparator implements RawComparator, Configurable {
    private static final ConcurrentHashMap<Class,WritableComparator> comparators=new ConcurrentHashMap<>();
    private Configuration conf;
    public static WritableComparator get(Class<? extends WritableComparable> c){
        return get(c,null);
    }
    public static WritableComparator get(Class<? extends WritableComparable> c,Configuration conf){
        WritableComparator comparator = comparators.get(c);
        if (comparator == null) {
            forceInit(c);
            comparator=comparators.get(c);
            if (comparator == null) {
                comparator=new WritableComparator(c,conf,true);
            }
        }
        ReflectionUtils.setConf(comparator,conf);
        return comparator;
    }

    private static void forceInit(Class<?> cls) {
        try {
            Class.forName(cls.getName(),true,cls.getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Can't initialize class "+cls,e);
        }
    }

    @Override
    public int compare(byte[] data1, int start1, int length1, byte[] data2, int start2, int length2) {
        return 0;
    }

    @Override
    public int compare(Object o1, Object o2) {
        return 0;
    }

    @Override
    public void setConf(Configuration conf) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }
}
