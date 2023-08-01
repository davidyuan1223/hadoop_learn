package org.apache.hadoop.security;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Credentials implements Writable {
    private static final Logger LOG= LoggerFactory.getLogger(Credentials.class);
    private Map<Text,byte[]> secretKeysMap=new HashMap<>();
    private Map<Text, Token<? extends TokenIdentifier>> tokenMap=new HashMap<>();
    public Credentials(){}
    public Credentials(Credentials credentials){
        this.addAll(credentials);
    }
    public Token<? extends TokenIdentifier> getToken(Text alias){
        return tokenMap.get(alias);
    }

    public enum SerializedFormat{
        WRITABLE((byte) 0x00),
        PROTOBUF((byte) 0x01);

        public static final SerializedFormat[] FORMATS=values();
        final byte value;
        SerializedFormat(byte val){
            this.value=val;
        }
        public static SerializedFormat valudOf(int val){
            try {
                return FORMATS[val];
            }catch (ArrayIndexOutOfBoundsException e){
                throw new IllegalArgumentException("Unknown credential format: "+val);
            }
        }
    }
}
