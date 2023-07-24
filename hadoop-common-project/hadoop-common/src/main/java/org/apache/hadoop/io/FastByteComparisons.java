package org.apache.hadoop.io;

import org.apache.hadoop.thirdparty.com.google.common.primitives.UnsignedBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/23
 **/
public abstract class FastByteComparisons {
    static final Logger logger= LoggerFactory.getLogger(FastByteComparisons.class);



    private interface Comparer<T>{
        abstract public int compareTo(T buffer1,int offset1,int length1,
                                      T buffer2,int offset2,int length2);
    }
    private static Comparer<byte[]> lexicographicalComparerJavaImpl(){
        return LexicographicalComparerHolder.PureJavaComparer.INSTANCE;
    }
    public static int compareTo(byte[] b1,int s1,int l1,
                                byte[] b2,int s2,int l2){
        return LexicographicalComparerHolder.BEST_COMPARER.compareTo(
                b1, s1, l1, b2, s2, l2);
    }
    private static class LexicographicalComparerHolder{
        static final String UNSAFE_COMPARER_NAME=
                LexicographicalComparerHolder.class.getName()+"$UnsafeComparer";
        static final Comparer<byte[]> BEST_COMPARER=getBestComparer();
        static Comparer<byte[]> getBestComparer(){
            if (System.getProperty("os.arch").toLowerCase().startsWith("sparc")) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Lexicographical comparer selected for " +
                            "byte aligned system architecture");
                }
                return lexicographicalComparerJavaImpl();
            }
            try {
                Class<?> clazz=Class.forName(UNSAFE_COMPARER_NAME);
                @SuppressWarnings("unchecked")
                Comparer<byte[]> comparer= (Comparer<byte[]>) clazz.getEnumConstants()[0];
                if (logger.isTraceEnabled()) {
                    logger.trace("Unsafe comparer selected for byte unaligned system architecture");
                }
                return comparer;
            }catch (Throwable t){
                if (logger.isTraceEnabled()) {
                    logger.trace(t.getMessage());
                    logger.trace("Lexicographical comparer selected");
                }
                return lexicographicalComparerJavaImpl();
            }
        }


        private enum PureJavaComparer implements Comparer<byte[]> {
            INSTANCE;

            @Override
            public int compareTo(byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2, int length2) {
                if (buffer1 == buffer2 &&
                        offset1 == offset2 &&
                        length1 == length2) {
                    return 0;
                }
                int end1=offset1+length1;
                int end2=offset2+length2;
                for(int i=offset1,j=offset2;i<end1&&j<end2;i++,j++){
                    int a=(buffer1[i]&0xff);
                    int b=(buffer2[j]&0xff);
                    if (a!=b) {
                        return a-b;
                    }
                }
                return length1-length2;
            }
        }
        @SuppressWarnings("unused")
        private enum UnsafeComparer implements Comparer<byte[]>{
            INSTANCE;
            static final Unsafe theUnsafe;
            static final int BYTE_ARRAY_BASE_OFFSET;
            static {
                theUnsafe=(Unsafe) AccessController.doPrivileged(new PrivilegedAction<Object>() {
                    @Override
                    public Object run(){
                        try {
                            Field f=Unsafe.class.getDeclaredField("theUnsafe");
                            f.setAccessible(true);
                            return f.get(null);
                        }catch (NoSuchFieldException|IllegalAccessException e){
                            throw new Error();
                        }
                    }
                });
                BYTE_ARRAY_BASE_OFFSET=theUnsafe.arrayBaseOffset(byte[].class);
                if (theUnsafe.arrayIndexScale(byte[].class) != 1) {
                    throw new AssertionError();
                }
            }

            static final boolean littleEndian=
                    ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);
            static boolean lessThanUnsigned(long x1,long x2){
                return (x1+Long.MIN_VALUE)<(x2+Long.MIN_VALUE);
            }

            @Override
            public int compareTo(byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2, int length2) {
                if (buffer1==buffer2&&
                offset1==offset2&&
                length1==length2){
                    return 0;
                }
                final int stride=8;
                int minLength = Math.min(length1, length2);
                int strideLimit = minLength & ~(stride - 1);
                int offset1Adj = offset1 + BYTE_ARRAY_BASE_OFFSET;
                int offset2Adj = offset2 + BYTE_ARRAY_BASE_OFFSET;
                int i;
                for (i = 0; i < strideLimit; i += stride) {
                    long lw = theUnsafe.getLong(buffer1, offset1Adj + (long) i);
                    long rw = theUnsafe.getLong(buffer2, offset2Adj + (long) i);

                    if (lw != rw) {
                        if (!littleEndian) {
                            return lessThanUnsigned(lw, rw) ? -1 : 1;
                        }
                        int n = Long.numberOfTrailingZeros(lw ^ rw) & ~0x7;
                        return ((int) ((lw >>> n) & 0xFF)) - ((int) ((rw >>> n) & 0xFF));
                    }
                }
                for (; i < minLength; i++) {
                    int result = UnsignedBytes.compare(
                            buffer1[offset1 + i],
                            buffer2[offset2 + i]);
                    if (result != 0) {
                        return result;
                    }
                }
                return length1 - length2;
            }
        }
    }
}
