package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.util.Arrays;
import java.util.Objects;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/23
 **/
@Deprecated
@InterfaceAudience.LimitedPrivate({"HDFS","MapReduce"})
@InterfaceStability.Stable
public class UTF8 implements WritableComparable<UTF8>{
    private static final Logger LOG= LoggerFactory.getLogger(UTF8.class);
    private static final DataInputBuffer IBUF=new DataInputBuffer();
    private static final ThreadLocal<DataOutputBuffer> OBUF_FACTOR=
            new ThreadLocal<DataOutputBuffer>(){
                @Override
                protected DataOutputBuffer initialValue() {
                    return new DataOutputBuffer();
                }
            };
    private static final byte[] EMPTY_BYTES=new byte[0];
    private byte[] bytes=EMPTY_BYTES;
    private int length;
    public UTF8(){}
    public UTF8(String str){set(str);}
    public UTF8(UTF8 utf8){set(utf8);}
    public byte[] getBytes(){return bytes;}
    public int getLength(){return length;}
    public void set(String str){
        if (str.length() > 0xffff/3){
            LOG.warn("truncating long string: "+str.length()
            +" chars,starting with "+str.substring(0,20));
            str=str.substring(0,0xffff/3);
        }
        length=utf8Length(str);
        if (length > 0xfffff) {
            throw new RuntimeException("string too long");
        }
        if (bytes == null || length > bytes.length) {
            bytes=new byte[length];
        }
        try {
            DataOutputBuffer oBuf = OBUF_FACTOR.get();
            oBuf.reset();
            writeChars(oBuf,str,0,str.length());
            System.arraycopy(oBuf.getData(),0,bytes,0,length);
        }catch (IOException e){
            throw new RuntimeException(e);
        }
    }
    public void set(UTF8 utf8){
        length= utf8.length;
        if (bytes == null || length > bytes.length) {
            bytes=new byte[length];
        }
        System.arraycopy(utf8.bytes,0,bytes,0,length);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        length=in.readUnsignedShort();
        if (bytes == null || bytes.length < length) {
            bytes=new byte[length];
        }
        in.readFully(bytes,0,length);
    }
    public static void skip(DataInput in) throws IOException {
        int length = in.readUnsignedShort();
        WritableUtils.skipFully(in,length);
    }

    @Override
    public void writer(DataOutput out) throws IOException {
        out.writeShort(length);
        out.write(bytes,0,length);
    }

    @Override
    public int compareTo(UTF8 o) {
        return WritableComparator.compareBytes(bytes,0,length,
                o.bytes,0,o.length);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(length);
        try {
            synchronized (IBUF){
                IBUF.reset(bytes,length);
                readChars(IBUF,sb,length);
            }
        }catch (IOException e){
            throw new RuntimeException(e);
        }
        return sb.toString();
    }
    public String toStringChecked()throws IOException{
        StringBuilder sb = new StringBuilder(length);
        synchronized (IBUF){
            IBUF.reset(bytes,length);
            readChars(IBUF,sb,length);
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof UTF8)) {
            return false;
        }
        UTF8 that= (UTF8) o;
        if (this.length != that.length) {
            return false;
        }else {
            return WritableComparator.compareBytes(bytes,0,length,
                    that.bytes, 0,that.length)==0;
        }
    }

    @Override
    public int hashCode() {
        return WritableComparator.hashBytes(bytes,length);
    }
    public static class Comparator extends WritableComparator{
        public Comparator(){super(UTF8.class);}

        @Override
        public int compare(byte[] b1, int start1, int length1, byte[] b2, int start2, int length2) {
            int n1=readUnsignedShort(b1,start1);
            int n2=readUnsignedShort(b2,start2);
            return compareBytes(b1,start1+2,n1,b2,start2+2,n2);
        }
    }
    static {
        WritableComparator.define(UTF8.class,new Comparator());
    }
    public static byte[] getBytes(String str){
        byte[] result=new byte[utf8Length(str)];
        try{
            DataOutputBuffer oBuf = OBUF_FACTOR.get();
            oBuf.reset();
            writeChars(oBuf,str,0,str.length());
            System.arraycopy(oBuf.getData(),0,result,0,oBuf.getLength(););
        }catch (IOException e){
            throw new RuntimeException(e);
        }
        return result;
    }
    public static String fromBytes(byte[] bytes)throws IOException{
        DataInputBuffer dbuf = new DataInputBuffer();
        dbuf.reset(bytes,0,bytes.length);
        StringBuilder sb = new StringBuilder(bytes.length);
        readChars(dbuf,sb,bytes.length);
        return sb.toString();
    }
    public static String readString(DataInput in)throws IOException{
        int bytes = in.readUnsignedShort();
        StringBuilder sb = new StringBuilder(bytes);
        readChars(in,sb,bytes);
        return sb.toString();
    }
    private static void readChars(DataInput in, StringBuilder buffer, int nBytes)
            throws UTFDataFormatException, IOException {
        DataOutputBuffer obuf = OBUF_FACTOR.get();
        obuf.reset();
        obuf.write(in, nBytes);
        byte[] bytes = obuf.getData();
        int i = 0;
        while (i < nBytes) {
            byte b = bytes[i++];
            if ((b & 0x80) == 0) {
                // 0b0xxxxxxx: 1-byte sequence
                buffer.append((char)(b & 0x7F));
            } else if ((b & 0xE0) == 0xC0) {
                if (i >= nBytes) {
                    throw new UTFDataFormatException("Truncated UTF8 at " +
                            StringUtils.byteToHexString(bytes, i - 1, 1));
                }
                // 0b110xxxxx: 2-byte sequence
                buffer.append((char)(((b & 0x1F) << 6)
                        | (bytes[i++] & 0x3F)));
            } else if ((b & 0xF0) == 0xE0) {
                // 0b1110xxxx: 3-byte sequence
                if (i + 1 >= nBytes) {
                    throw new UTFDataFormatException("Truncated UTF8 at " +
                            StringUtils.byteToHexString(bytes, i - 1, 2));
                }
                buffer.append((char)(((b & 0x0F) << 12)
                        | ((bytes[i++] & 0x3F) << 6)
                        |  (bytes[i++] & 0x3F)));
            } else if ((b & 0xF8) == 0xF0) {
                if (i + 2 >= nBytes) {
                    throw new UTFDataFormatException("Truncated UTF8 at " +
                            StringUtils.byteToHexString(bytes, i - 1, 3));
                }
                // 0b11110xxx: 4-byte sequence
                int codepoint =
                        ((b & 0x07) << 18)
                                | ((bytes[i++] & 0x3F) <<  12)
                                | ((bytes[i++] & 0x3F) <<  6)
                                | ((bytes[i++] & 0x3F));
                buffer.append(highSurrogate(codepoint))
                        .append(lowSurrogate(codepoint));
            } else {
                // The UTF8 standard describes 5-byte and 6-byte sequences, but
                // these are no longer allowed as of 2003 (see RFC 3629)

                // Only show the next 6 bytes max in the error code - in case the
                // buffer is large, this will prevent an exceedingly large message.
                int endForError = Math.min(i + 5, nBytes);
                throw new UTFDataFormatException("Invalid UTF8 at " +
                        StringUtils.byteToHexString(bytes, i - 1, endForError));
            }
        }
    }

    private static char highSurrogate(int codePoint) {
        return (char) ((codePoint >>> 10)
                + (Character.MIN_HIGH_SURROGATE - (Character.MIN_SUPPLEMENTARY_CODE_POINT >>> 10)));
    }

    private static char lowSurrogate(int codePoint) {
        return (char) ((codePoint & 0x3ff) + Character.MIN_LOW_SURROGATE);
    }

    /**
     * @return Write a UTF-8 encoded string.
     *
     * @see DataOutput#writeUTF(String)
     * @param out input out.
     * @param s input s.
     * @throws IOException raised on errors performing I/O.
     */
    public static int writeString(DataOutput out, String s) throws IOException {
        if (s.length() > 0xffff/3) {         // maybe too long
            LOG.warn("truncating long string: " + s.length()
                    + " chars, starting with " + s.substring(0, 20));
            s = s.substring(0, 0xffff/3);
        }

        int len = utf8Length(s);
        if (len > 0xffff)                             // double-check length
            throw new IOException("string too long!");

        out.writeShort(len);
        writeChars(out, s, 0, s.length());
        return len;
    }

    /** Returns the number of bytes required to write this. */
    private static int utf8Length(String string) {
        int stringLength = string.length();
        int utf8Length = 0;
        for (int i = 0; i < stringLength; i++) {
            int c = string.charAt(i);
            if (c <= 0x007F) {
                utf8Length++;
            } else if (c > 0x07FF) {
                utf8Length += 3;
            } else {
                utf8Length += 2;
            }
        }
        return utf8Length;
    }

    private static void writeChars(DataOutput out,
                                   String s, int start, int length)
            throws IOException {
        final int end = start + length;
        for (int i = start; i < end; i++) {
            int code = s.charAt(i);
            if (code <= 0x7F) {
                out.writeByte((byte)code);
            } else if (code <= 0x07FF) {
                out.writeByte((byte)(0xC0 | ((code >> 6) & 0x1F)));
                out.writeByte((byte)(0x80 |   code       & 0x3F));
            } else {
                out.writeByte((byte)(0xE0 | ((code >> 12) & 0X0F)));
                out.writeByte((byte)(0x80 | ((code >>  6) & 0x3F)));
                out.writeByte((byte)(0x80 |  (code        & 0x3F)));
            }
        }
    }
}
