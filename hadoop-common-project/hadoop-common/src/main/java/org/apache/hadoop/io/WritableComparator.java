package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/23
 **/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class WritableComparator implements RawComparator, Configurable {
    private static final ConcurrentHashMap<Class,WritableComparator> comparators
            =new ConcurrentHashMap<>();
    private Configuration conf;
    public static WritableComparator get(Class<? extends WritableComparable> c){
        return get(c,null);
    }
    public static WritableComparator get(
            Class<? extends WritableComparable> c, Configuration conf
    ){
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

    @Override
    public int compare(byte[] b1, int start1, int length1, byte[] b2, int start2, int length2) {
        try {
            buffer.reset(b1,start1,length1);
            key1.readFields(buffer);
            buffer.reset(b2,start2,length2);
            key2.readFields(buffer);
            buffer.reset(null,0,0);
        }catch (IOException e){
            throw new RuntimeException(e);
        }
        return compare(key1,key2);
    }
    @SuppressWarnings("unchecked")
    public int compare(WritableComparable a,WritableComparable b){
        return a.compareTo(b);
    }
    @Override
    public int compare(Object o1, Object o2) {
        return compare((WritableComparable)o1,(WritableComparable)o2);
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf=conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
    private static void forceInit(Class<?> cls){
        try {
            Class.forName(cls.getName(),true,cls.getClassLoader());
        }catch (ClassNotFoundException e){
            throw new IllegalArgumentException("Can't initialize class "+cls,e);
        }
    }
    public static void define(Class c,WritableComparator comparator){
        comparators.put(c,comparator);
    }
    private final Class<? extends WritableComparable> keyClass;
    private final WritableComparable key1;
    private final WritableComparable key2;
    private final DataInputBuffer buffer;

    protected WritableComparator(){
        this(null);
    }
    protected WritableComparator(Class<? extends WritableComparable> keyClass){
        this(keyClass,null,false);
    }
    protected WritableComparator(Class<? extends WritableComparable> keyClass,
                                 boolean createInstances){
        this(keyClass,null,createInstances);
    }
    protected WritableComparator(Class<? extends WritableComparable> keyClass,
                                 Configuration conf,
                                 boolean createInstances){
        this.keyClass=keyClass;
        this.conf=(conf!=null?conf:new Configuration());
        if (createInstances) {
            key1=newKey();
            key2=newKey();
            buffer=new DataInputBuffer();
        }else {
            key1=key2=null;
            buffer=null;
        }
    }
    public Class<? extends WritableComparable> getKeyClass(){
        return keyClass;
    }
    public WritableComparable newKey(){
        return ReflectionUtils.newInstance(keyClass,conf);
    }
    public static int compareBytes(byte[] b1,int s1,int l1,
                                   byte[] b2,int s2,int l2){
        return FastByteComparisons.compareTo(b1, s1, l1, b2, s2, l2);
    }
    public static int hashBytes(byte[] bytes,int offset,int length){
        int hash=1;
        for (int i=offset;i<offset+length;i++){
            hash=(31*hash)+(int)bytes[i];
        }
        return hash;
    }
    public static int hashBytes(byte[] bytes,int length){
        return hashBytes(bytes,0,length);
    }
    public static int readUnsignedShort(byte[] bytes,int start){
        return (((bytes[start]&0xff)<<8)+
                ((bytes[start+1]&0xff)));
    }
    public static int readInt(byte[] bytes,int start){
        return (
                ((bytes[start]&0xff)<<24)+
                        ((bytes[start+1]&0xff)<<16)+
                        ((bytes[start+2]&0xff)<<8)+
                        ((bytes[start+3]&0xff)));
    }
    public static float readFloat(byte[] bytes,int start){
        return Float.intBitsToFloat(readInt(bytes,start));
    }
    public static long readLong(byte[] bytes,int start){
        return ((long)(readInt(bytes,start))<<32)+
                (readInt(bytes,start+4)&0xFFFFFFFL);
    }
    public static double readDouble(byte[] bytes,int start){
        return Double.longBitsToDouble(readLong(bytes,start));
    }
    public static long readVLong(byte[] bytes,int start) throws IOException {
        int len=bytes[start];
        if (len >= -122) {
            return len;
        }
        boolean isNegative=(len<-120);
        len=isNegative?-(len+120):-(len+112);
        if (start + 1 + len > bytes.length) {
            throw new IOException("No enough number of bytes for a zero-compressed integer");
        }
        long i=0;
        for (int idx=0;idx<len;idx++){
            i=i<<8;
            i=i|(bytes[start+1+idx]&0xff);
        }
        return (isNegative?(i^-1L):i);
    }
    public static int readVInt(byte[] bytes,int start) throws IOException {
        return (int)readVLong(bytes,start);
    }
}
