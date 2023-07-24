package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.HadoopIllegalArgumentException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/23
 **/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ArrayPrimitiveWritable implements Writable{
    private Class<?> componentType=null;
    private Class<?> declaredComponentType=null;
    private int length;
    private Object value;
    private static final Map<String, Class<?>> PRIMITIVE_NAMES =
            new HashMap<String, Class<?>>(16);
    static {
        PRIMITIVE_NAMES.put(boolean.class.getName(), boolean.class);
        PRIMITIVE_NAMES.put(byte.class.getName(), byte.class);
        PRIMITIVE_NAMES.put(char.class.getName(), char.class);
        PRIMITIVE_NAMES.put(short.class.getName(), short.class);
        PRIMITIVE_NAMES.put(int.class.getName(), int.class);
        PRIMITIVE_NAMES.put(long.class.getName(), long.class);
        PRIMITIVE_NAMES.put(float.class.getName(), float.class);
        PRIMITIVE_NAMES.put(double.class.getName(), double.class);
    }
    private static Class<?> getPrimitiveClass(String classname){
        return PRIMITIVE_NAMES.get(classname);
    }
    private static void checkPrimitive(Class<?> componentType){
        if (componentType == null) {
            throw new HadoopIllegalArgumentException("null component type not allowed");
        }
        if (!PRIMITIVE_NAMES.containsKey(componentType.getName())) {
            throw new HadoopIllegalArgumentException("input array component type "
            +componentType.getName()+" is not a candidate primitive type");
        }
    }
    private void checkDeclaredComponentType(Class<?> componentType){
        if ((declaredComponentType != null)
                && (componentType != declaredComponentType)) {
            throw new HadoopIllegalArgumentException("input array component type "+
                    componentType.getName()+" does not match declared type "
            +declaredComponentType.getName());
        }
    }
    private static void checkArray(Object value){
        if (value == null) {
            throw new HadoopIllegalArgumentException("null value not allowed");
        }
        if (! value.getClass().isArray()){
            throw new HadoopIllegalArgumentException("non-array value of class"
            +value.getClass()+" not allowed");
        }
    }
    public ArrayPrimitiveWritable(){}
    public ArrayPrimitiveWritable(Class<?> componentType){
        checkPrimitive(componentType);
        this.declaredComponentType=componentType;
    }
    public ArrayPrimitiveWritable(Object value){
        set(value);
    }
    public Object get(){return value;}

    public Class<?> getComponentType() {
        return componentType;
    }

    public Class<?> getDeclaredComponentType() {
        return declaredComponentType;
    }
    public boolean isDeclaredComponentType(Class<?> componentType){
        return componentType==declaredComponentType;
    }
    public void set(Object value){
        checkArray(value);
        Class<?> componentType = value.getClass().getComponentType();
        checkPrimitive(componentType);
        checkDeclaredComponentType(componentType);
        this.componentType=componentType;
        this.value=value;
        this.length= Array.getLength(value);
    }

    @Override
    @SuppressWarnings("deprecation")
    public void writer(DataOutput out) throws IOException {
        UTF8.writeString(out,componentType.getName());
        out.writeInt(length);
        if (componentType == Boolean.TYPE) {          // boolean
            writeBooleanArray(out);
        } else if (componentType == Character.TYPE) { // char
            writeCharArray(out);
        } else if (componentType == Byte.TYPE) {      // byte
            writeByteArray(out);
        } else if (componentType == Short.TYPE) {     // short
            writeShortArray(out);
        } else if (componentType == Integer.TYPE) {   // int
            writeIntArray(out);
        } else if (componentType == Long.TYPE) {      // long
            writeLongArray(out);
        } else if (componentType == Float.TYPE) {     // float
            writeFloatArray(out);
        } else if (componentType == Double.TYPE) {    // double
            writeDoubleArray(out);
        } else {
            throw new IOException("Component type " + componentType.toString()
                    + " is set as the output type, but no encoding is implemented for this type.");
        }
    }

    private void writeDoubleArray(DataOutput out) throws IOException {
        double[] v=(double[])value;
        for (int i = 0; i < length; i++) {
            out.writeDouble(v[i]);
        }
    }

    private void writeFloatArray(DataOutput out) throws IOException {
        float[] v=(float[])value;
        for (int i = 0; i < length; i++) {
            out.writeFloat(v[i]);
        }
    }

    private void writeLongArray(DataOutput out) throws IOException {
        long[] v=(long[])value;
        for (int i = 0; i < length; i++) {
            out.writeLong(v[i]);
        }
    }

    private void writeIntArray(DataOutput out) throws IOException {
        int[] v=(int[])value;
        for (int i = 0; i < length; i++) {
            out.writeInt(v[i]);
        }
    }

    private void writeShortArray(DataOutput out) throws IOException {
        short[] v=(short[])value;
        for (int i = 0; i < length; i++) {
            out.writeShort(v[i]);
        }
    }

    private void writeByteArray(DataOutput out) throws IOException {
        byte[] v=(byte[])value;
        for (int i = 0; i < length; i++) {
            out.writeByte(v[i]);
        }
    }

    private void writeCharArray(DataOutput out) throws IOException {
        char[] v=(char[]) value;
        for (int i = 0; i < length; i++) {
            out.writeChar(v[i]);
        }
    }

    private void writeBooleanArray(DataOutput out) throws IOException {
        boolean[] v=(boolean[])value;
        for (int i = 0; i < length; i++) {
            out.writeBoolean(v[i]);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        @SuppressWarnings("deprecation")
        String classname=UTF8.readString(in);
        Class<?> componentType = getPrimitiveClass(classname);
        if (componentType == null) {
            throw new HadoopIllegalArgumentException("encoded array component type "
            +classname+" is not a candidate primitive type");
        }
        checkDeclaredComponentType(componentType);
        this.componentType=componentType;
        int length = in.readInt();
        if (length < 0) {
            throw new IOException("encoded array length is negative "+length);
        }
        this.length=length;
        value=Array.newInstance(componentType,length);
        if (componentType == Boolean.TYPE) {             // boolean
            readBooleanArray(in);
        } else if (componentType == Character.TYPE) {    // char
            readCharArray(in);
        } else if (componentType == Byte.TYPE) {         // byte
            readByteArray(in);
        } else if (componentType == Short.TYPE) {        // short
            readShortArray(in);
        } else if (componentType == Integer.TYPE) {      // int
            readIntArray(in);
        } else if (componentType == Long.TYPE) {         // long
            readLongArray(in);
        } else if (componentType == Float.TYPE) {        // float
            readFloatArray(in);
        } else if (componentType == Double.TYPE) {       // double
            readDoubleArray(in);
        } else {
            throw new IOException("Encoded type " + classname
                    + " converted to valid component type " + componentType.toString()
                    + " but no encoding is implemented for this type.");
        }
    }
    private void readBooleanArray(DataInput in) throws IOException {
        boolean[] v = (boolean[]) value;
        for (int i = 0; i < length; i++)
            v[i] = in.readBoolean();
    }

    private void readCharArray(DataInput in) throws IOException {
        char[] v = (char[]) value;
        for (int i = 0; i < length; i++)
            v[i] = in.readChar();
    }

    private void readByteArray(DataInput in) throws IOException {
        in.readFully((byte[]) value, 0, length);
    }

    private void readShortArray(DataInput in) throws IOException {
        short[] v = (short[]) value;
        for (int i = 0; i < length; i++)
            v[i] = in.readShort();
    }

    private void readIntArray(DataInput in) throws IOException {
        int[] v = (int[]) value;
        for (int i = 0; i < length; i++)
            v[i] = in.readInt();
    }

    private void readLongArray(DataInput in) throws IOException {
        long[] v = (long[]) value;
        for (int i = 0; i < length; i++)
            v[i] = in.readLong();
    }

    private void readFloatArray(DataInput in) throws IOException {
        float[] v = (float[]) value;
        for (int i = 0; i < length; i++)
            v[i] = in.readFloat();
    }

    private void readDoubleArray(DataInput in) throws IOException {
        double[] v = (double[]) value;
        for (int i = 0; i < length; i++)
            v[i] = in.readDouble();
    }
    static class Internal extends ArrayPrimitiveWritable{
        Internal(){super();}
        Internal(Object value){super(value);}
    }
}
