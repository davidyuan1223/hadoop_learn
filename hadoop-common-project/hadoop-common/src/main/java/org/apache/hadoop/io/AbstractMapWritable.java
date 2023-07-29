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
    Map<Class<?>,Byte> classToIdMap=new ConcurrentHashMap<>();
    @VisibleForTesting
    Map<Byte,Class<?>> idToClassMap=new ConcurrentHashMap<>();
    private volatile byte newClasses=0;
    byte getNewClasses(){return newClasses;}
    private synchronized void addToMap(Class<?> clazz,byte b){
        if (classToIdMap.containsKey(clazz)) {
            byte id = classToIdMap.get(clazz);
            if (id != b) {
                throw new IllegalArgumentException("Class "+clazz.getName()
                +" already registered but maps to "+id+" and not "+b);
            }
        }
        if (idToClassMap.containsKey(b)) {
            Class<?> c = idToClassMap.get(b);
            if (!c.equals(clazz)) {
                throw new IllegalArgumentException("Id "+b+" exists but maps to "+
                        c.getName()+" and not "+clazz.getName());
            }
        }
        classToIdMap.put(clazz,b);
        idToClassMap.put(b,clazz);
    }
    protected synchronized void addToMap(Class<?> clazz){
        if (classToIdMap.containsKey(clazz)) {
            return;
        }
        if (newClasses + 1 > Byte.MAX_VALUE) {
            throw new IndexOutOfBoundsException("adding an additional class would" +
                    " exceed the maximum number allowed");
        }
        byte id=++newClasses;
        addToMap(clazz,id);
    }
    protected Class<?> getClass(byte id){
        return idToClassMap.get(id);
    }
    private byte getId(Class<?> clazz){
        return classToIdMap.containsKey(clazz)?classToIdMap.get(clazz):-1;
    }
    protected synchronized void copy(Writable other){
        if (other != null) {
            try {
                DataOutputBuffer out = new DataOutputBuffer();
                other.writer(out);
                DataInputBuffer in = new DataInputBuffer();
                in.reset(out.getData(),out.getLength());
                readFields(in);
            }catch (IOException e){
                throw new IllegalArgumentException("map cannot be copied: "+
                        e.getMessage());
            }
        }else {
            throw new IllegalArgumentException("source map cannot be null");
        }
    }
    protected AbstractMapWritable(){
        this.conf=new AtomicReference<>();
        addToMap(ArrayWritable.class,(byte) -127);
        addToMap(BooleanWritable.class,(byte) -126);
        addToMap(BytesWritable.class,(byte) -125);
        addToMap(FloatWritable.class,(byte) -124);
        addToMap(IntWritable.class,(byte) -123);
        addToMap(LongWritable.class,(byte) -122);
        addToMap(MapWritable.class,(byte) -121);
        addToMap(MD5Hash.class,(byte) -120);
        addToMap(NullWritable.class,(byte) -119);
        addToMap(ObjectWritable.class,(byte) -118);
        addToMap(SortedMapWritable.class,(byte) -117);
        addToMap(Text.class,(byte) -116);
        addToMap(TwoDArrayWritable.class,(byte) -115);
        addToMap(VIntWritable.class,(byte) -114);
        addToMap(VLongWritable.class,(byte) -113);
    }

    @Override
    public Configuration getConf() {
        return conf.get();
    }

    public void setConf(Configuration conf) {
        this.conf.set(conf);
    }

    @Override
    public void writer(DataOutput out) throws IOException {
        out.writeByte(newClasses);
        for (byte i = 0; i < newClasses; i++) {
            out.writeByte(i);
            out.writeUTF(getClass(i).getName());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        newClasses=in.readByte();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        for (byte i = 0; i < newClasses; i++) {
            byte id = in.readByte();
            String className=in.readUTF();
            try {
                addToMap(classLoader.loadClass(className),id);
            }catch (ClassNotFoundException e){
                throw new IOException(e);
            }
        }
    }
}




