package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

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
        this(Text.class,new Writable[strs.length]);
        for (int i = 0; i < strs.length; i++) {
            values[i]=new UTF8(strs[i]);
        }
    }
    public Class<? extends Writable> getValueClass(){
        return valueClass;
    }
    public String[] toStrings(){
        String[] strs=new String[values.length];
        for (int i = 0; i < values.length; i++) {
            strs[i]=values[i].toString();
        }
        return strs;
    }
    public Object toArray(){
        return Arrays.copyOf(values,values.length);
    }
    public void set(Writable[] values){
        this.values=values;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        values=new Writable[in.readInt()];
        for (int i = 0; i < values.length; i++) {
            Writable value = WritableFactories.newInstance(valueClass);
            value.readFields(in);
            values[i]=value;
        }
    }

    @Override
    public void writer(DataOutput out) throws IOException {
        out.writeInt(values.length);
        for (int i = 0; i < values.length; i++) {
            values[i].writer(out);
        }
    }

    @Override
    public String toString() {
        return "ArrayWritable [valueClass="+valueClass+
                ",values="+Arrays.toString(values)+"]";
    }
}
