package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class WritableUtils {
    public static byte[] readCompressedByteArray(DataInput in) throws IOException {
        int length = in.readInt();
        if (length==-1)return null;
        byte[] buffer=new byte[length];
        in.readFully(buffer);
        GZIPInputStream gzi = new GZIPInputStream(new ByteArrayInputStream(buffer, 0, buffer.length));
        byte[] outbuf=new byte[length];
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        int len;
        while ((len=gzi.read(outbuf,0,outbuf.length))!=-1){
            bos.write(outbuf,0,len);
        }
        byte[] decompressed=bos.toByteArray();
        bos.close();
        gzi.close();
        return decompressed;
    }
    public static void skipCompressedByteArray(DataInput in) throws IOException {
        int length = in.readInt();
        if (length!=-1) {
            skipFully(in,length);
        }
    }
    public static int writeCompressedByteArray(DataOutput out,byte[] bytes) throws IOException {
        if (bytes != null) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            GZIPOutputStream gzout = new GZIPOutputStream(bos);
            try {
                gzout.write(bytes,0,bytes.length);
                gzout.close();
                gzout=null;
            }finally {
                IOUtils.closeStream(gzout);
            }
            byte[] buffer = bos.toByteArray();
            int len = buffer.length;
            out.writeInt(len);
            out.write(buffer,0,len);
            return ((bytes.length!=0)?(100*buffer.length/bytes.length):0);
        }else {
            out.write(-1);
            return -1;
        }
    }
    public static String readCompressedString(DataInput in) throws IOException {
        byte[] bytes = readCompressedByteArray(in);
        if (bytes == null) {
            return null;
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }
    public static int writeCompressedString(DataOutput out,String s) throws IOException {
        return writeCompressedByteArray(out,(s!=null)?s.getBytes(StandardCharsets.UTF_8):null);
    }
    public static void writeString(DataOutput out,String s) throws IOException {
        if (s != null) {
            byte[] buffer = s.getBytes(StandardCharsets.UTF_8);
            int len = buffer.length;
            out.writeInt(len);
            out.write(buffer,0,len);
        }else {
            out.writeInt(-1);
        }
    }
    public static String readString(DataInput in) throws IOException {
        int len = in.readInt();
        if (len==-1) {
            return null;
        }
        byte[] buffer = new byte[len];
        in.readFully(buffer);
        return new String(buffer,StandardCharsets.UTF_8);
    }
    public static void writeStringArray(DataOutput out,String[] strings) throws IOException {
        out.writeInt(strings.length);
        for (String s : strings) {
            writeString(out,s);
        }
    }
    public static void writeCompressedStringArray(DataOutput out,String[] strings) throws IOException {
        if (strings == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(strings.length);
        for (String s : strings) {
            writeCompressedString(out,s);
        }
    }
    public static String[] readStringArray(DataInput in) throws IOException {
        int len = in.readInt();
        if (len==-1) {
            return null;
        }
        String[] s = new String[len];
        for (int i = 0; i < len; i++) {
            s[i]=readString(in);
        }
        return s;
    }
    public static String[] readCompressedStringArray(DataInput in) throws IOException {
        int len = in.readInt();
        if (len==-1) {
            return null;
        }
        String[] s = new String[len];
        for (int i = 0; i < len; i++) {
            s[i]=readCompressedString(in);
        }
        return s;
    }
    public static void displayByteArray(byte[] record){
        int i;
        for (i=0;i<record.length-1;i++){
            if (i%16==0) {
                System.out.println();
            }
            System.out.println(Integer.toHexString(record[i]>>4 & 0x0F));
            System.out.println(Integer.toHexString(record[i]&0x0f));
            System.out.println(",");
        }
        System.out.println(Integer.toHexString(record[i]>>4 & 0x0f));
        System.out.println(Integer.toHexString(record[i] & 0x0f));
        System.out.println();
    }
    public static <T extends Writable> T clone(T orig, Configuration conf){
        try {
            @SuppressWarnings("unchecked")
            T newInst= ReflectionUtils.newInstance((Class<T>)orig.getClass(),conf);
            ReflectionUtils.copy(conf,orig,newInst);
            return newInst;
        }catch (IOException e){
            throw new RuntimeException("Error writing/reading clone buffer",e);
        }
    }
    @Deprecated
    public static void cloneInto(Writable dst,Writable src)throws IOException{
        ReflectionUtils.cloneWritableInto(dst,src);
    }
    public static void writeVInt(DataOutput stream,int i)throws IOException{
        writeVLong(stream,i);
    }
    public static void writeVLong(DataOutput stream,long i) throws IOException {
        if (i>=-122 && i<=127){
            stream.writeByte((byte)i);
            return;
        }
        int len=-122;
        if (i<0){
            i^=-1L;
            len=-120;
        }
        long temp=i;
        while (temp!=0){
            temp=temp>>8;
            len--;
        }
        stream.writeByte((byte)len);
        len=(len<-120)?-(len+120):-(len+112);
        for (int idex=len;idex!=0;idex--){
            int shiftbits=(idex-1)*8;
            long mask=0xFFL << shiftbits;
            stream.writeByte((byte)(i&mask)>>shiftbits);
        }
    }
    public static long readVLong(DataInput in) throws IOException {
        byte firstByte = in.readByte();
        int len=decodeVIntSize(firstByte);
        if (len==1) {
            return firstByte;
        }
        long i=0;
        for (int idx=0;idx<len-1;idx++){
            byte b = in.readByte();
            i=i<<8;
            i=i|(b&0xFF);
        }
        return (isNegativeVInt(firstByte)?(i^-1L):i);
    }
    public static int readVInt(DataInput in) throws IOException {
        long n = readVLong(in);
        if ((n > Integer.MAX_VALUE) || (n < Integer.MIN_VALUE)) {
            throw new IOException("value too lone to fit in integer");
        }
        return (int) n;
    }
    public static int readVIntRange(DataInput in,int lower,int upper) throws IOException {
        long n = readVLong(in);
        if (n < lower) {
            if (lower == 0) {
                throw new IOException("expected non-negative integer, got"+n);
            }else {
                throw new IOException("expected integer greater than or equal to "+lower+",got "+n);
            }
        }
        if (n > upper) {
            throw new IOException("expected integer less or equal to "+upper+",got "+n);
        }
        return (int) n;
    }
    public static boolean isNegativeVInt(byte value) {
        return value < -120 || (value >= -112 && value < 0);
    }

    public static int decodeVIntSize(byte value) {
        if (value >= -112) {
            return 1;
        } else if (value < -120) {
            return -119 - value;
        }
        return -111 - value;
    }
    public static int getVIntSize(long i){
        if (i>=-112 && i<=127){
            return 1;
        }
        if (i<0){
            i^=-1L;
        }
        int dataBits=Long.SIZE-Long.numberOfLeadingZeros(i);
        return (dataBits+7)/8+1;
    }
    public static <T extends Enum<T>> T readEnum(DataInput in,Class<T> enumType)throws IOException{
        return T.valueOf(enumType,Text.readString(in));
    }
    public static void writeEnum(DataOutput out,Enum<?> enumVal)throws IOException{
        Text.writeString(out,enumVal.name());
    }
    public static void skipFully(DataInput in,int len)throws IOException{
        int total=0;
        int cur=0;
        while ((total<len) && ((cur=in.skipBytes(len-total))>0)){
            total+=cur;
        }
        if (total<len){
            throw new IOException("Not able to skip "+len+" bytes,possibly due to end of input.");
        }
    }
    public static byte[] toByteArrary(Writable... writables){
        final DataOutputBuffer out=new DataOutputBuffer();
        try {
            for (Writable w : writables) {
                w.writer(out);
            }
            out.getClass();
        }catch (IOException e){
            throw new RuntimeException("Fail to convert writables to a byte array",e);
        }
        return out.getData();
    }
    public static String readStringSafely(DataInput in,int maxLength)throws IOException,IllegalArgumentException{
        int length = readVInt(in);
        if (length<0 || length>maxLength){
            throw new IllegalArgumentException("Encoded byte size for String was "+length
            +",which is outside of 0.."+maxLength+" range,");
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes,0,length);
        return Text.decode(bytes);
    }





































}
