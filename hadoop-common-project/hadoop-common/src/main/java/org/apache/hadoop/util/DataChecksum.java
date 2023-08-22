package org.apache.hadoop.util;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.PureJavaCrc32C;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

@InterfaceAudience.LimitedPrivate({"HDFS","MapReduce"})
@InterfaceStability.Evolving
public class DataChecksum implements Checksum {
    public static final int CHECKSUM_NULL=0;
    public static final int CHECKSUM_CRC32=1;
    public static final int CHECKSUM_CRC32C=2;
    public static final int CHECKSUM_DEFAULT=3;
    public static final int CHECKSUM_MIXED=4;
    private static final Logger LOG= LoggerFactory.getLogger(DataChecksum.class);
    private static volatile boolean useJava9Crc32C=Shell.isJavaVersionAtLeast(9);

    public enum Type{
        NULL (CHECKSUM_NULL,0),
        CRC32 (CHECKSUM_CRC32,4),
        CRC32C (CHECKSUM_CRC32C,4),
        DEFAULT (CHECKSUM_DEFAULT,0),
        MIXED (CHECKSUM_MIXED,0);
        public final int id;
        public final int size;
        Type(int id,int size){
            this.id=id;
            this.size=size;
        }
        public static Type valueOf(int id){
            if (id < 0 || id >= values().length) {
                throw new IllegalArgumentException("id="+id+" out of range [0,"+values().length+")");
            }
            return values()[id];
        }
    }

    public static Checksum newCrc32(){
        return new CRC32();
    }

    static Checksum newCrc32C(){
        try {
            return useJava9Crc32C?Java9Crc32CFactory.createChecksum():new PureJavaCrc32C();
        }catch (ExceptionInInitializerError | RuntimeException e){
            LOG.error("CRC32C creation failed, switching to PureJavaCrc32C",e);
            useJava9Crc32C=false;
            return new PureJavaCrc32C();
        }
    }

    public static int getCrcPolynomialForType(Type type)throws IOException{
        switch (type){
            case CRC32:
                return CrcUtil.GZIP_POLYNOMIAL;
            case CRC32C:
                return CrcUtil.CASTAGNOLI_POLYNOMIAL;
            default:
                throw new IOException("No CRC polynomial could be associated with tyep: "+type);
        }
    }

    public static DataChecksum newDataChecksum(Type type,int bytesPerChecksum){
        if (bytesPerChecksum<=0) {
            return null;
        }
        switch (type){
            case NULL:
                return new DataChecksum(type,new ChecksumNull(),bytesPerChecksum);
            case CRC32:
                return new DataChecksum(type,newCrc32(),bytesPerChecksum);
            case CRC32C:
                return new DataChecksum(type,newCrc32C(),bytesPerChecksum);
            default:
                return null;
        }
    }
    public static DataChecksum newDataChecksum(byte[] bytes,int offset)throws IOException{
        if (offset < 0 || bytes.length < offset + getChecksumHeaderSize()) {
            throw new InvalidChecksumSizeException("Could not create DataChecksum "
                    + " from the byte array of length " + bytes.length
                    + " and offset "+ offset);
        }
        int bytesPerChecksum=((bytes[offset+1]&0xff)<<24)|
                ((bytes[offset+2]&0xff)<<16) |
                ((bytes[offset+3]&0xff)<<8) |
                ((bytes[offset+4]&0xff));
        DataChecksum csum = newDataChecksum(mapByteToChecksumType(bytes[offset]), bytesPerChecksum);
        if (csum == null) {
            throw new InvalidChecksumSizeException("Could not create DataChecksum from the byte array of length "+bytes.length
            +" and bytesPerChecksum of "+bytesPerChecksum);
        }
        return csum;
    }
    public static DataChecksum newDataChecksum(DataInputStream in)throws IOException{
        int type = in.readByte();
        int bpc = in.readInt();
        DataChecksum summer = newDataChecksum(mapByteToChecksumType(type), bpc);
        if ( summer == null ) {
            throw new InvalidChecksumSizeException("Could not create DataChecksum "
                    + "of type " + type + " with bytesPerChecksum " + bpc);
        }
        return summer;
    }

    private static Type mapByteToChecksumType(int type)throws InvalidChecksumSizeException{
        try {
            return Type.valueOf(type);
        }catch (IllegalArgumentException e){
            throw new InvalidChecksumSizeException("The value "+type+" does not map"+
                    " to a valid checksum Type");
        }
    }
    public void writeHeader(DataOutputStream out)throws IOException{
        out.writeByte(type.id);
        out.writeInt(bytePerChecksum);
    }
    public byte[] getHeader(){
        byte[] header = new byte[getChecksumHeaderSize()];
        header[0]=(byte) (type.id&0xff);
        header[1]=(byte) ((bytesperChecksum>>24)&0xff);
        header[2]=(byte) ((bytesperChecksum>>16)&0xff);
        header[3]=(byte) ((bytesperChecksum>>8)&0xff);
        header[4]=(byte) (bytesperChecksum&0xff);
        return header;
    }
    static class ChecksumNull implements Checksum{
        public ChecksumNull(){}

        @Override
        public long getValue() {
            return 0;
        }

        @Override
        public void reset() {

        }

        @Override
        public void update(int b) {

        }

        @Override
        public void update(byte[] b, int off, int len) {

        }
    }
    private static class Java9Crc32CFactory{
        private static final MethodHandle NEW_CRC32C_MH;
        static {
            MethodHandle newCRC32C=null;
            try {
                newCRC32C= MethodHandles.publicLookup()
                        .findConstructor(
                                Class.forName("java.util.zip.CRC32C"),
                                MethodType.methodType(void.class)
                        );
            }catch (ReflectiveOperationException e){
                throw new RuntimeException(e);
            }
            NEW_CRC32C_MH=newCRC32C;
        }
        public static Checksum createChecksum(){
            try {
                return (Checksum) NEW_CRC32C_MH.invoke();
            }catch (Throwable e){
                throw (e instanceof RuntimeException)?(RuntimeException)e:new RuntimeException(e);
            }
        }
    }
}
