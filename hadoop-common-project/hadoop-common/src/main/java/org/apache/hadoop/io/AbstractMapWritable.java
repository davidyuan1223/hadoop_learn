package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import com.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class AbstractMapWritable implements Writable, Configurable {
    private AtomicReference<Configuration> conf;

    @VisibleForTesting
    Map<Class<?>,Byte> classToIdMap = new ConcurrentHashMap<>();

    @VisibleForTesting
    Map<Byte,Class<?>> idToClassMap = new ConcurrentHashMap<>();

    private volatile byte newClasses=0;
    byte getNewClasses(){return newClasses;}

    private synchronized void addToMap(Class<?> clazz,byte id){
        if (classToIdMap.containsKey(clazz)) {
            Byte b = classToIdMap.get(clazz);
            if (b != id) {
                throw new IllegalArgumentException("Class "+clazz.getName()+
                        " already registered but maps to "+b+" and not "+id);
            }
        }
        if (idToClassMap.containsKey(id)) {
            Class<?> c = idToClassMap.get(id);
            if (!c.equals(clazz)) {
                throw new IllegalArgumentException("Id "+id+" exists but maps to"+c.getName()
                +" and not "+clazz.getName());
            }
        }
        classToIdMap.put(clazz,id);
        idToClassMap.put(id,clazz);
    }

    private synchronized void addToMap(Class<?> clazz){
        if (classToIdMap.containsKey(clazz)) {
            return;
        }
        if (newClasses + 1 > Byte.MAX_VALUE) {
            throw new IndexOutOfBoundsException("adding an additional class would exceed the maximum number allowed");
        }
        byte id=++newClasses;
        addToMap(clazz,id);
    }

    protected Class<?> getClass(byte id){return idToClassMap.get(id);}

    protected byte getId(Class<?> clazz){return classToIdMap.containsKey(clazz)?classToIdMap.get(clazz):-1;}

    protected synchronized void copy(Writable other){
        if (other != null) {
            try {
                DataOutputBuffer out = new DataOutputBuffer();
                other.write(out);
                DataInputBuffer in = new DataInputBuffer();
                in.reset(out.getData(),out.getLength());
                readFields(in);
            }catch (IOException e){
                throw new IllegalArgumentException("map cannot be copied: "+e.getMessage());
            }
        }else {
            throw new IllegalArgumentException("source map cannot be null");
        }
    }

    protected AbstractMapWritable(){
        this.conf=new AtomicReference<>();

    }
    @Override
    public void setConf(Configuration conf) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }

    @Override
    public void write(DataOutput output) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }
}
