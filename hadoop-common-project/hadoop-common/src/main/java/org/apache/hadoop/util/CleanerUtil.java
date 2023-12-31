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
import java.util.Objects;

import static java.lang.invoke.MethodHandles.*;
import static java.lang.invoke.MethodType.methodType;

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
                final MethodHandle unmapper=lookup.findVirtual(unsafeClass,"invokeCleaner", methodType(void.class,ByteBuffer.class));
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
                final MethodHandle cleanMethod=lookup.findVirtual(cleanerClass,"clean",methodType(void.class));
                final MethodHandle nonNullTest=lookup.findStatic(Objects.class,"nonNull",methodType(boolean.class,Objects.class))
                        .asType(methodType(boolean.class,cleanerClass));
                final MethodHandle noop=dropArguments(constant(Void.class,null).asType(methodType(void.class)),0,cleanerClass);
                final MethodHandle unmapper=filterReturnValue(directBufferCleanerMethod,guardWithTest(nonNullTest,cleanMethod,noop)
                        .asType(methodType(void.class,ByteBuffer.class)));
                return newBufferCleaner(directBufferClass,unmapper);
            }
        }catch (SecurityException e){
            return "Unmapping is not supported, because not all required " +
                    "permissions are given to the Hadoop JAR file: " + e +
                    " [Please grant at least the following permissions: " +
                    "RuntimePermission(\"accessClassInPackage.sun.misc\") " +
                    " and ReflectPermission(\"suppressAccessChecks\")]";
        }catch (ReflectiveOperationException | RuntimeException e) {
            return "Unmapping is not supported on this platform, " +
                    "because internal Java APIs are not compatible with " +
                    "this Hadoop version: " + e;
        }
    }

    private static BufferCleaner newBufferCleaner(final Class<?> unmappableBufferClass,
                                                  final MethodHandle unmapper){
        assert Objects.equals(methodType(void.class,ByteBuffer.class),unmapper.type());
        return buffer->{
            if (!buffer.isDirect()) {
                throw new IllegalArgumentException("unmapping only works with direct buffers");
            }
            if (!unmappableBufferClass.isInstance(buffer)) {
                throw new IllegalArgumentException("buffer is not an instance of "+
                        unmappableBufferClass.getName());
            }
            final Throwable error=AccessController.doPrivileged(
                    (PrivilegedAction<? extends Throwable>) ()->{
                        try {
                            unmapper.invokeExact(buffer);
                            return null;
                        }catch (Throwable t){
                            return t;
                        }
                    }
            );
            if (error != null) {
                throw new IOException("Unable to unmap the mapper buffer",error);
            }
        };
    }

    @FunctionalInterface
    public interface BufferCleaner{
        void freeBuffer(ByteBuffer b)throws IOException;
    }
}
