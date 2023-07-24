package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import com.jcraft.jsch.IO;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.thirdparty.com.google.common.base.Utf8;
import org.apache.hadoop.thirdparty.protobuf.Message;
import org.apache.hadoop.util.ProtoUtil;
import org.apache.kerby.config.Conf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/23
 **/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ObjectWritable implements Writable, Configurable {
    private Class declaredClass;
    private Object instance;
    private Configuration conf;
    public ObjectWritable(){}
    public ObjectWritable(Object instance){set(instance);}
    public ObjectWritable(Class declaredClass,Object instance){
        this.declaredClass=declaredClass;
        this.instance=instance;
    }
    public Object get(){return instance;}
    public Class getDeclaredClass(){return declaredClass;}
    public void set(Object instance){
        this.declaredClass=instance.getClass();
        this.instance=instance;
    }

    @Override
    public String toString() {
        return "OW[class="+declaredClass+",value="+instance+"]";
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        readObject(in,this,this.conf);
    }

    @Override
    public void writer(DataOutput out) throws IOException {
        writeObject(out,instance,declaredClass,conf);
    }
    private static final Map<String ,Class<?>> PRIMITIVE_NAMES=new HashMap<>();
    static {
        PRIMITIVE_NAMES.put("boolean",Boolean.TYPE);
        PRIMITIVE_NAMES.put("byte",Byte.TYPE);
        PRIMITIVE_NAMES.put("char",Character.TYPE);
        PRIMITIVE_NAMES.put("short",Short.TYPE);
        PRIMITIVE_NAMES.put("int",Integer.TYPE);
        PRIMITIVE_NAMES.put("long",Long.TYPE);
        PRIMITIVE_NAMES.put("float",Float.TYPE);
        PRIMITIVE_NAMES.put("double",Double.TYPE);
        PRIMITIVE_NAMES.put("void",Void.TYPE);
    }
    private static class NullInstance extends Configured implements Writable{
        private Class<?> declaredClass;
        public NullInstance(){super(null);}
        public NullInstance(Class declaredClass,Configuration conf){
            super(conf);
            this.declaredClass=declaredClass;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            String className= UTF8.readString(in);
            declaredClass=PRIMITIVE_NAMES.get(className);
            if (declaredClass == null) {
                try {
                    declaredClass=getConf().getClassByName(className);
                }catch (ClassNotFoundException e){
                    throw new RuntimeException(e.toString());
                }
            }
        }

        @Override
        public void writer(DataOutput out) throws IOException {
            UTF8.writeString(out,declaredClass.getName());
        }
    }
    public static void writeObject(DataOutput out,Object instance,
                                   Class declaredClass,
                                   Configuration conf){
        writeObject(out,instance,declaredClass,conf,false);
    }
    public static void writeObject(DataOutput out, Object instance,
                                   Class declaredClass, Configuration conf,
                                   boolean allowCompactArrays) throws IOException {
        if (instance == null) {
            instance=new NullInstance(declaredClass,conf);
            declaredClass=Writable.class;
        }
        if (allowCompactArrays && declaredClass.isArray()
                && instance.getClass().getName().equals(declaredClass.getName())
                && instance.getClass().getComponentType().isPrimitive()) {
            instance=new ArrayPrimitiveWritable.Internal(instance);
            declaredClass=ArrayPrimitiveWritable.Internal.class;
        }
        UTF8.writeString(out,declaredClass.getName());
        if (declaredClass.isArray()) {
            int length = Array.getLength(instance);
            out.writeInt(length);
            for (int i = 0; i < length; i++) {
                writeObject(out,Array.get(instance,i),
                        declaredClass.getComponentType(),conf,allowCompactArrays);
            }
        }else if (declaredClass==ArrayPrimitiveWritable.Internal.class){
            ((ArrayPrimitiveWritable.Internal)instance).write(out);
        }else if (declaredClass==String.class){
            UTF8.writeString(out,(String)instance);
        } else if (declaredClass.isPrimitive()) {
            if (declaredClass == Boolean.TYPE) {
                out.writeBoolean(((Boolean) instance).booleanValue());
            } else if (declaredClass == Character.TYPE) {
                out.writeChar(((Character)instance).charValue());
            }else if (declaredClass == Byte.TYPE) {
                out.writeByte(((Byte)instance).byteValue());
            }else if (declaredClass == Short.TYPE) {
                out.writeShort(((Short)instance).shortValue());
            }else if (declaredClass == Integer.TYPE) {
                out.writeInt(((Integer)instance).intValue());
            }else if (declaredClass == Long.TYPE) {
                out.writeLong(((Long)instance).longValue());
            }else if (declaredClass == Float.TYPE) {
                out.writeFloat(((Float)instance).floatValue());
            }else if (declaredClass == Double.TYPE) {
                out.writeDouble(((Double)instance).doubleValue());
            }else if (declaredClass == Void.TYPE) {
            }else {
                throw new IllegalArgumentException("Not a primitive: "+declaredClass);
            }
        } else if (declaredClass.isEnum()) {
            UTF8.writeString(out,((Enum)instance).name());
        } else if (Writable.class.isAssignableFrom(declaredClass)) {
            UTF8.writeString(out,instance.getClass().getName());
            ((Writable)instance).writer(out);
        } else if (Message.class.isAssignableFrom(declaredClass)) {
            ((Message)instance).writeDelimitedTo(
                    DataOutputOutputStream.constructOutputStream(out)
            );
        }else {
            throw new IOException("Can't write: "+instance+" as "+declaredClass);
        }
    }
    public static Object readObject(DataInput in,Configuration conf){
        return readObject(in,null,conf);
    }
    @SuppressWarnings("unchecked")
    public static Object readObject(DataInput in,ObjectWritable objectWritable,Configuration conf) throws IOException {
        String className=UTF8.readString(in);
        Class<?> declaredClass = PRIMITIVE_NAMES.get(className);
        if (declaredClass == null) {
            declaredClass=loadClass(conf,className);
        }
        Object instance;
        if (declaredClass.isPrimitive()) {
            if (declaredClass == Boolean.TYPE) {             // boolean
                instance = Boolean.valueOf(in.readBoolean());
            } else if (declaredClass == Character.TYPE) {    // char
                instance = Character.valueOf(in.readChar());
            } else if (declaredClass == Byte.TYPE) {         // byte
                instance = Byte.valueOf(in.readByte());
            } else if (declaredClass == Short.TYPE) {        // short
                instance = Short.valueOf(in.readShort());
            } else if (declaredClass == Integer.TYPE) {      // int
                instance = Integer.valueOf(in.readInt());
            } else if (declaredClass == Long.TYPE) {         // long
                instance = Long.valueOf(in.readLong());
            } else if (declaredClass == Float.TYPE) {        // float
                instance = Float.valueOf(in.readFloat());
            } else if (declaredClass == Double.TYPE) {       // double
                instance = Double.valueOf(in.readDouble());
            } else if (declaredClass == Void.TYPE) {         // void
                instance = null;
            } else {
                throw new IllegalArgumentException("Not a primitive: "+declaredClass);
            }
        } else if (declaredClass.isArray()) {
            int length=in.readInt();
            instance=Array.newInstance(declaredClass.getComponentType(),length);
            for (int i = 0; i < length; i++) {
                Array.set(instance,i,readObject(in,conf));
            }
        }else if (declaredClass==ArrayPrimitiveWritable.Internal.class){
            ArrayPrimitiveWritable.Internal temp=new ArrayPrimitiveWritable.Internal();
            temp.readFields(in);
            instance=temp.get();
            declaredClass=instance.getClass();
        } else if (declaredClass == String.class) {
            instance=UTF8.readStrinfg(in);
        } else if (declaredClass.isEnum()) {
            instance=Enum.valueOf((Class<? extends Enum>)declaredClass,UTF8.readString(in));
        } else if (Message.class.isAssignableFrom(declaredClass)) {
            instance=tryInstantiateProtobuf(declaredClass,in);
        }else {
            Class instanceClass=null;
            String str=UTF8.readString(in);
            instanceClass=loadClass(conf,str);
            Writable writable=WritableFactories.newInstance(instanceClass,conf);
            writable.readFields(in);
            instance=writable;
            if (instanceClass == NullInstance.class) {
                declaredClass=((NullInstance)instance).declaredClass;
                instance=null;
            }
        }
        if (objectWritable != null) {
            objectWritable.declaredClass=declaredClass;
            objectWritable.instance=instance;
        }
        return instance;
    }
    private static Message tryInstantiateProtobuf(Class<?> protoClass,
                                                  DataInput in) throws IOException {
        try {
            if (in instanceof InputStream) {
                Method parsedMethod=getStaticProtobufMethod(protoClass,
                        "parseDelimitedFrom",InputStream.class);
                return (Message) parsedMethod.invoke(null,in);
            }else {
                int size= ProtoUtil.readRawVarint32(in);
                if (size < 0) {
                    throw new IOException("Invalid size: "+size);
                }
                byte[] data=new byte[size];
                in.readFully(data);
                Method parsedMethod=getStaticProtobufMethod(protoClass,
                        "parseFrom",byte[].class);
                return (Message) parsedMethod.invoke(null,data);
            }
        }catch (InvocationTargetException e){
            if (e.getCause() instanceof IOException) {
                throw (IOException)e.getCause();
            }else {
                throw new IOException(e.getCause());
            }
        }catch (IllegalAccessException e){
            throw new AssertionError("Could not access parse method in"+protoClass);
        }
    }
    static Method getStaticProtobufMethod(Class<?> declaredClass,
                                          String method,
                                          Class<?> ... args){
        try {
            return declaredClass.getMethod(method,args);
        }catch (Exception e){
            throw new AssertionError("Protocl buffer class "+declaredClass
            +" does not have an accessible parseFrom(InputStream) method!");
        }
    }
    public static Class<?> loadClass(Configuration conf,String className){
        Class<?> declaredClass=null;
        try {
            if (conf != null) {
                declaredClass=conf.getClassByName(className);
            }else {
                declaredClass=Class.forName(className);
            }
        }catch (ClassNotFoundException e){
            throw new RuntimeException("readObject can't find class "+className,e);
        }
        return declaredClass;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
