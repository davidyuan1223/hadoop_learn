package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.avro.reflect.Stringable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.*;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.Arrays;
import java.util.Objects;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/23
 **/
@Stringable
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Text extends BinaryComparable implements WritableComparable<BinaryComparable> {
    private static final ThreadLocal<CharsetEncoder> ENCODER_FATORY=
            new ThreadLocal<CharsetEncoder>(){
                @Override
                protected CharsetEncoder initialValue() {
                    return StandardCharsets.UTF_8.newEncoder()
                            .onMalformedInput(CodingErrorAction.REPORT)
                            .onUnmappableCharacter(CodingErrorAction.REPORT);
                }
            };
    private static final ThreadLocal<CharsetDecoder> DECODER_FACTORY=new ThreadLocal<>(){
        @Override
        protected Object initialValue() {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT);
        }
    };
    private static final int ARRAY_MAX_SIZE=Integer.MAX_VALUE-8;
    private static final byte[] EMPTY_BYTES=new byte[0];
    private byte[] bytes=EMPTY_BYTES;
    private int length=0;
    private int textLength=-1;

    public Text(){}
    public Text(String str){set(str);}
    public Text(Text utf8){set(utf8);}
    public Text(byte[] utf8){set(utf8);}
    public byte[] copyBytes(){
        return Arrays.copyOf(bytes,length);
    }

    @Override
    public byte[] getBytes() {
        return bytes;
    }

    @Override
    public int getLength() {
        return length;
    }
    public int getTextLength(){
        if (textLength<0){
            textLength=toString().length();
        }
        return textLength;
    }
    public int charAt(int position){
        if (position>this.length) return -1;
        if (position<0) return -1;
        ByteBuffer bb= (ByteBuffer) ByteBuffer.wrap(bytes).position(position);
        return byteToCodePoint(bb.slice());
    }
    public int find(String what){
        return find(what,0);
    }
    public int find(String what,int start){
        try {
            ByteBuffer src=ByteBuffer.wrap(this.bytes,0,this.length);
            ByteBuffer tgt=encode(what);
            byte b=tgt.get();
            src.position(start);
            while (src.hasRemaining()) {
                if (b==src.get()) {
                    src.mark();
                    tgt.mark();
                    boolean found=true;
                    int pos=src.position()-1;
                    while (tgt.hasRemaining()) {
                        if (!src.hasRemaining()) {
                            tgt.reset();
                            src.reset();
                            found=false;
                            break;
                        }
                        if (!(tgt.get() == src.get())) {
                            tgt.reset();
                            src.reset();
                            found=false;
                            break;
                        }
                    }
                    if (found) return pos;
                }
            }
            return -1;
        }catch (CharacterCodingException e){
            throw new RuntimeException("Should not have happened",e);
        }
    }
    public void set(String str){
        try {
            ByteBuffer bb=encode(str,true);
            bytes=bb.array();
            length=bb.limit();
            textLength=str.length();
        }catch (CharacterCodingException e){
            throw new RuntimeException("Should not have happened",e);
        }
    }
    public void set(byte[] utf8){
        if (utf8.length==0) {
            bytes=EMPTY_BYTES;
            length=0;
            textLength=-1;
        }else {
            set(utf8,0,utf8.length);
        }
    }
    public void set(Text other){
        set(other.getBytes(),0,other.getLength());
        this.textLength=other.textLength;
    }
    public void set(byte[] utf8,int start,int len){
        ensureCapacity(len);
        System.arraycopy(utf8,start,bytes,0,len);
        this.length=len;
        this.textLength=-1;
    }
    public void append(byte[] utf8,int start,int len){
        byte[] original=bytes;
        if (ensureCapacity(length + len)) {
            System.arraycopy(original,0,bytes,0,length);
        }
        System.arraycopy(utf8,start,bytes,length,len);
        length+=len;
        textLength=-1;
    }
    public void clear(){
        length=0;
        textLength=-1;
    }
    private boolean ensureCapacity(final int capacity){
        if (bytes.length < capacity) {
            long targetSizeLong=bytes.length+(bytes.length>>1);
            int targetSize=(int) Math.min(targetSizeLong,ARRAY_MAX_SIZE);
            targetSize=Math.max(capacity,targetSize);
            bytes=new byte[targetSize];
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        try {
            return decode(bytes,0,length);
        }catch (CharacterCodingException e){
            throw new RuntimeException("Should not have happened",e);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int newLength = WritableUtils.readVInt(in);
        readWithKnownLength(in,newLength);
    }
    public void readFields(DataInput in, int maxLength)throws IOException{
        int newLength = WritableUtils.readVInt(in);
        if (newLength < 0) {
            throw new IOException("tried to deserialize "+newLength+" bytes of data! newLength must be non-negative.");
        } else if (newLength >= maxLength) {
            throw new IOException("tried to deserialize "+newLength+
                    "bytes of data,but maxLength = "+maxLength);
        }
        readWithKnownLength(in,newLength);
    }
    public static void skip(DataInput in) throws IOException {
        int length = WritableUtils.readVInt(in);
        WritableUtils.skipFully(in,length);
    }
    public void readWithKnownLength(DataInput in,int len) throws IOException {
        ensureCapacity(len);
        in.readFully(bytes,0,len);
        length=len;
        textLength=-1;
    }

    @Override
    public void writer(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out,length);
        out.write(bytes,0,length);
    }
    public void write(DataOutput out,int maxLength)throws IOException{
        if (length > maxLength) {
            throw new IOException("data was to long to write! Expected less than or equal to "
            +maxLength+" bytes,but got "+length+" bytes.");
        }
        WritableUtils.writeVInt(out,length);
        out.write(bytes,0,length);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Text){
            return super.equals(o);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    public static class Comparator extends WritableComparator{
        public Comparator(){super(Text.class);}

        @Override
        public int compare(byte[] b1, int start1, int length1, byte[] b2, int start2, int length2) {
            int n1 = WritableUtils.decodeVIntSize(b1[start1]);
            int n2 = WritableUtils.decodeVIntSize(b2[start2]);
            return compareBytes(b1,start1+n1,length1-n1,b2,start2+n2,length2-n2);
        }
    }
    static {
        WritableComparator.define(Text.class,new Comparator());
    }
    public static String decode(byte[] utf8)throws CharacterCodingException{
        return decode(ByteBuffer.wrap(utf8),true);
    }
    public static String decode(byte[] utf8,int start,int length)throws CharacterCodingException{
        return decode(ByteBuffer.wrap(utf8,start,length),true);
    }
    public static String decode(byte[] utf8,int start,int length,boolean replace)throws CharacterCodingException{
        return decode(ByteBuffer.wrap(utf8,start,length),replace);
    }
    private static String decode(ByteBuffer utf8,boolean replace) throws CharacterCodingException {
        CharsetDecoder decoder = DECODER_FACTORY.get();
        if (replace) {
            decoder.onMalformedInput(
                    CodingErrorAction.REPLACE
            );
            decoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
        }
        String str = decoder.decode(utf8).toString();
        if (replace) {
            decoder.onMalformedInput(CodingErrorAction.REPORT);
            decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
        }
        return str;
    }
    public static ByteBuffer encode(String str) throws CharacterCodingException {
        return encode(str,true);
    }
    public static ByteBuffer encode(String str,boolean replace) throws CharacterCodingException {
        CharsetEncoder encoder = ENCODER_FATORY.get();
        if (replace) {
            encoder.onMalformedInput(CodingErrorAction.REPLACE);
            encoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
        }
        ByteBuffer bytes=encoder.encode(CharBuffer.wrap(str.toCharArray()));
        if (replace) {
            encoder.onMalformedInput(CodingErrorAction.REPORT);
            encoder.onUnmappableCharacter(CodingErrorAction.REPORT);
        }
        return bytes;
    }
    public static final int DEFAULT_MAX_LEN=1024*1024;
    public static String readString(DataInput in)throws IOException{
        return readString(in,Integer.MAX_VALUE);
    }
    public static String readString(DataInput in,int maxLength)throws IOException{
        int length=WritableUtils.readVIntRange(in,0,maxLength);
        byte[] bytes = new byte[length];
        in.readFully(bytes,0,length);
        return decode(bytes);
    }
    public static int writeString(DataOutput out,String s)throws IOException{
        ByteBuffer bytes = encode(s);
        int length = bytes.limit();
        WritableUtils.writeVInt(out,length);
        out.write(bytes.array(),0,length);
        return length;
    }
    public static int writeString(DataOutput out,String s,int maxLength)throws IOException{
        ByteBuffer bytes = encode(s);
        int length = bytes.limit();
        if (length > maxLength) {
            throw new IOException("string was too long to write! Expected "+
                    "less than or equal to "+maxLength+" bytes,but got "+
                    length+" bytes.");
        }
        WritableUtils.writeVInt(out,length);
        out.write(bytes.array(),0,length);
        return length;
    }
    private static final int LEAD_BYTE=0;
    private static final int TRAIL_BYTE_1=1;
    private static final int TRAIL_BYTE=2;
    public static void validateUTF8(byte[] utf8)throws MalformedInputException{
        validateUTF8(utf8,0,utf8.length);
    }
    public static void validateUTF8(byte[] utf8, int start, int len)
            throws MalformedInputException {
        int count = start;
        int leadByte = 0;
        int length = 0;
        int state = LEAD_BYTE;
        while (count < start+len) {
            int aByte = utf8[count] & 0xFF;

            switch (state) {
                case LEAD_BYTE:
                    leadByte = aByte;
                    length = bytesFromUTF8[aByte];

                    switch (length) {
                        case 0: // check for ASCII
                            if (leadByte > 0x7F)
                                throw new MalformedInputException(count);
                            break;
                        case 1:
                            if (leadByte < 0xC2 || leadByte > 0xDF)
                                throw new MalformedInputException(count);
                            state = TRAIL_BYTE_1;
                            break;
                        case 2:
                            if (leadByte < 0xE0 || leadByte > 0xEF)
                                throw new MalformedInputException(count);
                            state = TRAIL_BYTE_1;
                            break;
                        case 3:
                            if (leadByte < 0xF0 || leadByte > 0xF4)
                                throw new MalformedInputException(count);
                            state = TRAIL_BYTE_1;
                            break;
                        default:
                            // too long! Longest valid UTF-8 is 4 bytes (lead + three)
                            // or if < 0 we got a trail byte in the lead byte position
                            throw new MalformedInputException(count);
                    } // switch (length)
                    break;

                case TRAIL_BYTE_1:
                    if (leadByte == 0xF0 && aByte < 0x90)
                        throw new MalformedInputException(count);
                    if (leadByte == 0xF4 && aByte > 0x8F)
                        throw new MalformedInputException(count);
                    if (leadByte == 0xE0 && aByte < 0xA0)
                        throw new MalformedInputException(count);
                    if (leadByte == 0xED && aByte > 0x9F)
                        throw new MalformedInputException(count);
                    // falls through to regular trail-byte test!!
                case TRAIL_BYTE:
                    if (aByte < 0x80 || aByte > 0xBF)
                        throw new MalformedInputException(count);
                    if (--length == 0) {
                        state = LEAD_BYTE;
                    } else {
                        state = TRAIL_BYTE;
                    }
                    break;
                default:
                    break;
            } // switch (state)
            count++;
        }
    }

    /**
     * Magic numbers for UTF-8. These are the number of bytes
     * that <em>follow</em> a given lead byte. Trailing bytes
     * have the value -1. The values 4 and 5 are presented in
     * this table, even though valid UTF-8 cannot include the
     * five and six byte sequences.
     */
    static final int[] bytesFromUTF8 =
            { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0,
                    // trail bytes
                    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3,
                    3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5 };

    /**
     * @return Returns the next code point at the current position in
     * the buffer. The buffer's position will be incremented.
     * Any mark set on this buffer will be changed by this method!
     *
     * @param bytes input bytes.
     */
    public static int bytesToCodePoint(ByteBuffer bytes) {
        bytes.mark();
        byte b = bytes.get();
        bytes.reset();
        int extraBytesToRead = bytesFromUTF8[(b & 0xFF)];
        if (extraBytesToRead < 0) return -1; // trailing byte!
        int ch = 0;

        switch (extraBytesToRead) {
            case 5: ch += (bytes.get() & 0xFF); ch <<= 6; /* remember, illegal UTF-8 */
            case 4: ch += (bytes.get() & 0xFF); ch <<= 6; /* remember, illegal UTF-8 */
            case 3: ch += (bytes.get() & 0xFF); ch <<= 6;
            case 2: ch += (bytes.get() & 0xFF); ch <<= 6;
            case 1: ch += (bytes.get() & 0xFF); ch <<= 6;
            case 0: ch += (bytes.get() & 0xFF);
        }
        ch -= offsetsFromUTF8[extraBytesToRead];

        return ch;
    }

    static final int offsetsFromUTF8[] =
            { 0x00000000, 0x00003080,
                    0x000E2080, 0x03C82080, 0xFA082080, 0x82082080 };

    /**
     * For the given string, returns the number of UTF-8 bytes
     * required to encode the string.
     * @param string text to encode
     * @return number of UTF-8 bytes required to encode
     */
    public static int utf8Length(String string) {
        CharacterIterator iter = new StringCharacterIterator(string);
        char ch = iter.first();
        int size = 0;
        while (ch != CharacterIterator.DONE) {
            if ((ch >= 0xD800) && (ch < 0xDC00)) {
                // surrogate pair?
                char trail = iter.next();
                if ((trail > 0xDBFF) && (trail < 0xE000)) {
                    // valid pair
                    size += 4;
                } else {
                    // invalid pair
                    size += 3;
                    iter.previous(); // rewind one
                }
            } else if (ch < 0x80) {
                size++;
            } else if (ch < 0x800) {
                size += 2;
            } else {
                // ch < 0x10000, that is, the largest char value
                size += 3;
            }
            ch = iter.next();
        }
        return size;
    }
}
