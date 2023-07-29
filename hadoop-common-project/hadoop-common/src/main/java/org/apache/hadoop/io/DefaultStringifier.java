package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.util.GenericsUtil;
import org.apache.kerby.config.Conf;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class DefaultStringifier<T> implements Stringifier<T>{
    private static final String SEPARATOR=",";
    private Serializer<T> serializer;
    private Deserializer<T> deserializer;
    private DataInputBuffer inBuf;
    private DataOutputBuffer outBuf;

    public DefaultStringifier(Configuration conf,Class<T> c){
        SerializationFactory factory=new SerializationFactory(conf);
        this.serializer=factory.getSerializer(c);
        this.deserializer=factory.getDeSerializer(c);
        this.inBuf=new DataInputBuffer();
        this.outBuf=new DataOutputBuffer();
        try {
            serializer.open(outBuf);
            deserializer.open(inBuf);
        }catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public T fromString(String str) throws IOException {
        try {
            byte[] bytes= Base64.decodeBase64(str.getBytes(StandardCharsets.UTF_8));
            inBuf.reset(bytes,bytes.length);
            T restored=deserializer.deserialize(null);
            return restored;
        }catch (UnsupportedCharsetException e){
            throw new IOException(e.toString());
        }
    }

    @Override
    public String toString(T obj) throws IOException {
        outBuf.reset();
        serializer.serialize(obj);
        byte[] buf=new byte[outBuf.getLength()];
        System.arraycopy(outBuf.getData(),0,buf,0,buf.length);
        return new String(Base64.encodeBase64(buf),StandardCharsets.UTF_8);
    }

    @Override
    public void close() throws IOException {
        inBuf.close();
        outBuf.close();
        deserializer.close();
        serializer.close();
    }

    public static <K> void store(Configuration conf,K item,String keyName)throws IOException{
        DefaultStringifier<K> stringifier=new DefaultStringifier<K>(conf, GenericsUtil.getClass(item));
        conf.set(keyName,stringifier.toString(item));
        stringifier.close();
    }
    public static <K> K load(Configuration conf,String keyName,Class<K> itemClass)throws IOException{
        DefaultStringifier<K> stringifier = new DefaultStringifier<>(conf, itemClass);
        try {
            String itemStr=conf.get(keyName);
            return stringifier.fromString(itemStr);
        }finally {
            stringifier.close();
        }
    }
    public static <K> void storeArray(Configuration conf,K[] items,String keyName)throws IOException{
        if (items.length==0) {
            throw new IndexOutOfBoundsException();
        }
        DefaultStringifier<K> stringifier=new DefaultStringifier<K>(conf,GenericsUtil.getCkass(items[0]));
        try {
            StringBuilder sb = new StringBuilder();
            for (K item : items) {
                sb.append(stringifier.toString(item))
                        .append(SEPARATOR);
            }
            conf.set(keyName,sb.toString());
        }finally {
            stringifier.close();
        }
    }
    public static <K> K[] loadArray(Configuration conf,String keyName,Class<K> itemClass)throws IOException{
        DefaultStringifier<K> stringifier=new DefaultStringifier<K>(conf,itemClass);
        try {
            String itemStr = conf.get(keyName);
            ArrayList<K> list=new ArrayList<>();
            String[] parts = itemStr.split(SEPARATOR);
            for (String part : parts) {
                if (!part.isEmpty()) {
                    list.add(stringifier.fromString(part));
                }
            }
            return GenericsUtil.toArray(itemStr,list);
        }finally {
            stringifier.close();
        }
    }
}
