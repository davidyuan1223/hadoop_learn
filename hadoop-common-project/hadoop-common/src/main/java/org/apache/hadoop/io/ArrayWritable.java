package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/23
 **/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ArrayWritable implements Writable {
    private final Class<? extends Writable> valueClass;
    private Writable[] values;

    public ArrayWritable(Class<? extends Writable> valueClass){
        if (valueClass == null) {
            throw new IllegalArgumentException("null valueClass");
        }
        this.valueClass=valueClass;
    }
    public ArrayWritable(Class<? extends Writable> valueClass,Writable[] values){
        this(valueClass);
        this.values=values;
    }
    public ArrayWritable(String[] strs){
        this(Text)
    }
}
