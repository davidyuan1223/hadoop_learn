package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/23
 **/
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class GenericWritable implements Writable, Configurable {
    private static final byte NOT_SET=-1;
    private byte type=NOT_SET;
    private Writable instance;
    private Configuration conf=null;
    public void set(Writable obj){
        instance=obj;
        Class<? extends Writable> instanceClass = instance.getClass();
        Class<? extends Writable>[] classes=getTypes();
        for (int i = 0; i < classes.length; i++) {
            Class<? extends Writable> clazz = classes[i];
            if (clazz.equals(instanceClass)) {
                type= (byte) i;
                return;
            }
        }
        throw new RuntimeException("The type of instance is " +
                instance.getClass()+", which is NOT registered.");
    }
    public Writable get(){return instance;}

    @Override
    public String toString() {
        return "GW["+(instance!=null?("class="+instance.getClass().getName()
        +",value="+instance.toString()):"(null)]");
    }
    abstract protected Class<? extends Writable>[] getTypes();

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        type=in.readByte();
        Class<? extends Writable> clazz = getTypes()[this.type & 0xff];
        try {
            instance= ReflectionUtils.newInstance(clazz,conf);
        }catch (Exception e){
            e.printStackTrace();
            throw new IOException("Cannot initialize the class: "+clazz);
        }
        instance.readFields(in);
    }

    @Override
    public void writer(DataOutput out) throws IOException {
        if (type == NOT_SET || instance == null) {
            throw new IOException("The GenericWritable has NOT been set correctly." +
                    "type="+type+",instance="+instance);
        }
        out.writeByte(type);
        instance.writer(out);
    }
}
