package org.apache.hadoop.util;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class CleanerUtil {
    private CleanerUtil(){}
    public static final boolean UNMAP_SUPPORTED;
    private static final String UNMAP_NOT_SUPPORTED_REASON;
    private static final BufferCleaner CLEANER;
    public static BufferCleaner getCleaner(){return CLEANER;}
    static {
        final Object hack= AccessController.doPrivileged(
                (PrivilegedAction<Object>)CleanerUtil::unmapHackImpl
        );
        if (hack instanceof BufferCleaner) {
            CLEANER=(BufferCleaner) hack;
            UNMAP_SUPPORTED=true;
            UNMAP_NOT_SUPPORTED_REASON=null;
        }else {
            CLEANER=null;
            UNMAP_SUPPORTED=false;
            UNMAP_NOT_SUPPORTED_REASON=hack.toString();
        }
    }
    private static Object unmapHackImpl(){
        final MethodHandles.Lookup lookup=MethodHandles.lookup();
        try {
            try {
                final Class<?> unsafeClass=Class.forName("sun.misc.Unsafe");
                final MethodHandle unmapper=lookup.findVirtual(unsafeClass,"invokeCleaner", MethodType.methodType(void.class,ByteBuffer.class));
                final Field f=unsafeClass.getDeclaredField("theUnsafe");
                f.setAccessible(true);
                final Object theUnsafe=f.get(null);
                return newBufferCleaner(ByteBuffer.class,unmapper.bindTo(theUnsafe));
            }catch (SecurityException e){
                throw e;
            }catch (ReflectiveOperationException | RuntimeException e){
                final Class<?> directBufferClass=Class.forName("java.nio.DirectByteBuffer");
                final Method m = directBufferClass.getMethod("cleaner");
                m.setAccessible(true);
                final MethodHandle directBufferCleanerMethod=lookup.unreflect(m);
                final Class<?> cleanerClass=directBufferCleanerMethod.type().returnType();

            }
        }
    }





    @FunctionalInterface
    public interface BufferCleaner{
        void freeBuffer(ByteBuffer b)throws IOException;
    }
}
