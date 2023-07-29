package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.Options;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class MapFile {
    private static final Logger LOG= LoggerFactory.getLogger(MapFile.class);
    public static final String INDEX_FILE_NAME="index";
    public static final String DATA_FILE_NAME="data";
    protected MapFile(){}
    public static class Writer implements Closeable{
        private SequenceFile.Writer data;
        private SequenceFile.Writer index;
        final private static String INDEX_INTERVAL="io.map.index.interval";
        private int indexInterval=128;
        private long size;
        private LongWritable position=new LongWritable();
        private WritableComparator comparator;
        private DataInputBuffer inBuf=new DataInputBuffer();
        private DataOutputBuffer outBuf=new DataOutputBuffer();
        private WritableComparable lastKey;

        private long lastIndexPos=-1;
        private long lastIndexKeyCount=Long.MIN_VALUE;
        @Deprecated
        public Writer(Configuration conf, FileSystem fs,String dirName,
                      Class<? extends WritableComparable> keyClass,
                      Class valClass)throws IOException{
            this(conf,new Path(dirName),keyClass(keyClass),valueClass(valClass));
        }
        @Deprecated
        public Writer(Configuration conf, FileSystem fs, String dirName,
                      Class<? extends WritableComparable> keyClass,
                      Class valClass, SequenceFile.CompressionType compress,
                      Progressable progress)throws IOException{
            this(conf,new Path(dirName),keyClass(keyClass),valueClass(valClass),
                    compression(compress),progressable(progress));
        }
        @Deprecated
        public Writer(Configuration conf, FileSystem fs, String dirName,
                      Class<? extends WritableComparable> keyClass,
                      Class valClass, SequenceFile.CompressionType compress,
                      CompressionCodec codec,Progressable progress)throws IOException{
            this(conf,new Path(dirName),keyClass(keyClass),valueClass(valClass),
                    compression(compress,codec),progressable(progress));
        }
        @Deprecated
        public Writer(Configuration conf, FileSystem fs, String dirName,
                      Class<? extends WritableComparable> keyClass,
                      Class valClass, SequenceFile.CompressionType compress)throws IOException{
            this(conf,new Path(dirName),keyClass(keyClass),valueClass(valClass),
                    compression(compress));
        }
        @Deprecated
        public Writer(Configuration conf, FileSystem fs, String dirName,
                        WritableComparator comparator,
                      Class valClass)throws IOException{
            this(conf,new Path(dirName),comparator(comparator),valueClass(valClass));
        }
        @Deprecated
        public Writer(Configuration conf, FileSystem fs, String dirName,
                      WritableComparator comparator, Class valClass,
                      SequenceFile.CompressionType compress) throws IOException {
            this(conf, new Path(dirName), comparator(comparator),
                    valueClass(valClass), compression(compress));
        }
        @Deprecated
        public Writer(Configuration conf, FileSystem fs, String dirName,
                      WritableComparator comparator, Class valClass,
                      SequenceFile.CompressionType compress,
                      Progressable progress) throws IOException {
            this(conf, new Path(dirName), comparator(comparator),
                    valueClass(valClass), compression(compress),
                    progressable(progress));
        }
        @Deprecated
        public Writer(Configuration conf, FileSystem fs, String dirName,
                      WritableComparator comparator, Class valClass,
                      SequenceFile.CompressionType compress, CompressionCodec codec,
                      Progressable progress) throws IOException {
            this(conf, new Path(dirName), comparator(comparator),
                    valueClass(valClass), compression(compress, codec),
                    progressable(progress));
        }

        public static interface Option extends SequenceFile.Writer.Option{}

        private static class KeyClassOption extends Options.ClassOption implements Option {
            KeyClassOption(Class<?> value){
                super(value);
            }
        }
        private static class ComparatorOption implements Option{
            private final WritableComparator value;
            ComparatorOption(WritableComparator value){
                this.value=value;
            }
            WritableComparator getValue(){return value;}
        }
        public static Option keyClass(Class<? extends WritableComparable> value){
            return new KeyClassOption(value);
        }
        public static Option comparator(WritableComparator value){
            return new ComparatorOption(value);
        }
        public static SequenceFile.Writer.Option valueClass(Class<?> value){
            return SequenceFile.Writer.valueClass(value);
        }
        public static SequenceFile.Writer.Option compression(SequenceFile.CompressionType type){
            return SequenceFile.Writer.compression(type);
        }
        public static SequenceFile.Writer.Option compression(SequenceFile.CompressionType type,
                                                             CompressionCodec codec){
            return SequenceFile.Writer.compression(type,codec);
        }
        public static SequenceFile.Writer.Option progressable(Progressable value){
            return SequenceFile.Writer.progressable(value);
        }

        @SuppressWarnings("unchecked")
        public Writer(Configuration conf,
                      Path dirName,
                      SequenceFile.Writer.Option... options)throws IOException{
            KeyClassOption keyClassOption=Options.getOption(KeyClassOption.class,options);
            ComparatorOption comparatorOption=Options.getOption(ComparatorOption.class,options);
            if ((keyClassOption == null) == (comparatorOption == null)) {
                throw new IllegalArgumentException("key class or comparator option must be set");
            }
            this.indexInterval=conf.get(INDEX_INTERVAL,this.indexInterval);
            Class<? extends WritableComparable> keyClass;
            if (keyClassOption == null) {
                this.comparator=comparatorOption.getValue();
                keyClass=comparator.getKeyClass();
            }else {
                keyClass=(Class<? extends WritableComparable>) keyClassOption.getValue();
                this.comparator=WritableComparator.get(keyClass,conf);
            }
            this.lastKey=comparator.newKey();
            FileSystem fs=dirName.getFileSystem(conf);
            if (!fs.mkdirs(dirName)) {
                throw new IOException("Mkdirs failed to create directory "+dirName);
            }
            Path dataFile=new Path(dirName,DATA_FILE_NAME);
            Path indexFile=new Path(dirName,INDEX_FILE_NAME);
            SequenceFile.Writer.Option[] dataOptions=
                    Options.prependOptions(options,
                            SequenceFile.Writer.file(dataFile),
                            SequenceFile.Writer.keyClass(keyClass));
            this.data=SequenceFile.createWriter(conf,dataOptions);

        }
    }
}
