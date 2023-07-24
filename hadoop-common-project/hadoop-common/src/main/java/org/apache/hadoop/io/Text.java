package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.avro.reflect.Stringable;

import java.nio.ByteBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

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

        }
    }
}
