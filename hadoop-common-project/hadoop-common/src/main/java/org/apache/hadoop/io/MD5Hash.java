package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class MD5Hash implements WritableComparable<MD5Hash>{
    public static final int MD5_LEN=16;
    private static final ThreadLocal<MessageDigest> DIGESTER_FACTORY=new ThreadLocal<MessageDigest>(){
        @Override
        protected MessageDigest initialValue() {
            try {
                return MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }
    };
    private byte[] digest;
    public MD5Hash(){
        this.digest=new byte[MD5_LEN];
    }
    public MD5Hash(String hex){
        setDigest(hex);
    }
    public MD5Hash(byte[] digest){
        if (digest.length != MD5_LEN) {
            throw new IllegalArgumentException("Wrong length: "+digest.length);
        }
        this.digest=digest;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        in.readFully(digest);
    }
    public static MD5Hash read(DataInput in)throws IOException{
        MD5Hash result = new MD5Hash();
        result.readFields(in);
        return result;
    }

    @Override
    public void writer(DataOutput out) throws IOException {
        out.write(digest);
    }
    public void set(MD5Hash that){
        System.arraycopy(that.digest,0,this.digest,0,MD5_LEN);
    }
    public byte[] getDigest(){return digest;}
    public static MD5Hash digest(byte[] data){
        return digest(data,0,data.length);
    }
    public static MessageDigest getDigester(){
        MessageDigest digester = DIGESTER_FACTORY.get();
        digester.reset();
        return digester;
    }
    public static MD5Hash digest(InputStream in) throws IOException {
        final byte[] buffer=new byte[4*1024];
        final MessageDigest digester=getDigester();
        for (int n;(n=in.read(buffer))!=-1;){
            digester.update(buffer,0,n);
        }
        return new MD5Hash(digester.digest());
    }
    public static MD5Hash digest(byte[] data,int start,int len){
        byte[] digest;
        MessageDigest digester = getDigester();
        digester.update(data,start,len);
        digest=digester.digest();
        return new MD5Hash(digest);
    }
    public static MD5Hash digest(byte[][] dataArr,int start,int len){
        byte[] digest;
        MessageDigest digester = getDigester();
        for (byte[] data : dataArr) {
            digester.update(data,start,len);
        }
        digest=digester.digest();
        return new MD5Hash(digest);
    }
    public static MD5Hash digest(String str){
        return digest(UTF8.getBytes(str));
    }
    public static MD5Hash digest(UTF8 utf8){
        return digest(utf8.getBytes(),0,utf8.getLength());
    }
    public long halfDigest(){
        long value=0;
        for (int i = 0; i < 8; i++) {
            value |=((digest[i]&0xffL)<<(8*(7-i)));
        }
        return value;
    }
    public int quarterDigest(){
        int value=0;
        for (int i = 0; i < 4; i++) {
            value |= ((digest[i]&0xff)<<(8*(3-i)));
        }
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MD5Hash)) {
            return false;
        }
        MD5Hash that=(MD5Hash) o;
        return Arrays.equals(this.digest,that.digest);
    }


    @Override
    public int hashCode() {
        return quarterDigest();
    }

    @Override
    public int compareTo(MD5Hash o) {
        return WritableComparator.compareBytes(this.digest,0,MD5_LEN,
                o.digest,0,MD5_LEN);
    }
    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(MD5Hash.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1, MD5_LEN, b2, s2, MD5_LEN);
        }
    }

    static {                                        // register this comparator
        WritableComparator.define(MD5Hash.class, new Comparator());
    }

    private static final char[] HEX_DIGITS =
            {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};

    /** Returns a string representation of this object. */
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder(MD5_LEN*2);
        for (int i = 0; i < MD5_LEN; i++) {
            int b = digest[i];
            buf.append(HEX_DIGITS[(b >> 4) & 0xf])
                    .append(HEX_DIGITS[b & 0xf]);
        }
        return buf.toString();
    }

    /**
     * Sets the digest value from a hex string.
     * @param hex hex.
     */
    public void setDigest(String hex) {
        if (hex.length() != MD5_LEN*2)
            throw new IllegalArgumentException("Wrong length: " + hex.length());
        byte[] digest = new byte[MD5_LEN];
        for (int i = 0; i < MD5_LEN; i++) {
            int j = i << 1;
            digest[i] = (byte)(charToNibble(hex.charAt(j)) << 4 |
                    charToNibble(hex.charAt(j+1)));
        }
        this.digest = digest;
    }

    private static final int charToNibble(char c) {
        if (c >= '0' && c <= '9') {
            return c - '0';
        } else if (c >= 'a' && c <= 'f') {
            return 0xa + (c - 'a');
        } else if (c >= 'A' && c <= 'F') {
            return 0xA + (c - 'A');
        } else {
            throw new RuntimeException("Not a hex character: " + c);
        }
    }

}
