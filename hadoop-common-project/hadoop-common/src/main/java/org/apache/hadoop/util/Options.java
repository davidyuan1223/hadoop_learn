package org.apache.hadoop.util;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Arrays;

public class Options {

    public static abstract class StringOption{
        private final String value;
        protected StringOption(String value){
            this.value=value;
        }

        public String getValue() {
            return value;
        }
    }
    public static abstract class ClassOption{
        private final Class<?> value;
        protected ClassOption(Class<?> value){
            this.value=value;
        }

        public Class<?> getValue() {
            return value;
        }
    }
    public static abstract class BooleanOption{
        private final boolean value;
        protected BooleanOption(boolean value){
            this.value=value;
        }
        public boolean getValue(){
            return value;
        }
    }
    public static abstract class IntegerOption{
        private final int value;
        protected IntegerOption(int value){
            this.value=value;
        }

        public int getValue() {
            return value;
        }
    }
    public static abstract class LongOption{
        private final long value;
        protected LongOption(long value){
            this.value=value;
        }

        public long getValue() {
            return value;
        }
    }
    public static abstract class PathOption{
        private final Path value;
        protected PathOption(Path value){
            this.value=value;
        }

        public Path getValue() {
            return value;
        }
    }
    public static abstract class FSDataInputStreamOption{
        private final FSDataInputStream value;
        protected FSDataInputStreamOption(FSDataInputStream value){
            this.value=value;
        }

        public FSDataInputStream getValue() {
            return value;
        }
    }
    public static abstract class FSDataOutputStreamOption{
        private final FSDataOutputStream value;
        protected FSDataOutputStreamOption(FSDataOutputStream value){
            this.value=value;
        }

        public FSDataOutputStream getValue() {
            return value;
        }
    }
    public static abstract class ProgressableOption{
        private final Progressable value;
        protected ProgressableOption(Progressable value){
            this.value=value;
        }

        public Progressable getValue() {
            return value;
        }
    }
    @SuppressWarnings("unchecked")
    public static <base,T extends base> T getOption(Class<T> cls,base[] opts)throws IOException{
        for (base o : opts) {
            if (o.getClass() == cls) {
                return (T) o;
            }
        }
        return null;
    }
    public static <T> T[] prependOptions(T[] oldOpts,T... newOpts){
        T[] result= Arrays.copyOf(newOpts,newOpts.length+oldOpts.length);
        System.arraycopy(oldOpts,0,result,newOpts.length,oldOpts.length);
        return result;
    }
}
