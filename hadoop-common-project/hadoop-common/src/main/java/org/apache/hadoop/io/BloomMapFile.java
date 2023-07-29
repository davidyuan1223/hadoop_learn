package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import com.jcraft.jsch.HASH;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.Hash;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.bloom.DynamicBloomFilter;
import org.apache.hadoop.util.bloom.Filter;
import org.apache.hadoop.util.bloom.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class BloomMapFile {
    private static final Logger LOG= LoggerFactory.getLogger(BloomMapFile.class);
    public static final String BLOOM_FILE_NAME="bloom";
    public static final int HASH_COUNT=5;
    public static void delete(FileSystem fs,String name)throws IOException{
        Path dir=new Path(name);
        Path data=new Path(dir,MapFile.DATA_FILE_NAME);
        Path index=new Path(dir,MapFile.INDEX_FILE_NAME);
        Path bloom=new Path(dir,BLOOM_FILE_NAME);

        fs.delete(data,true);
        fs.delete(index,true);
        fs.delete(bloom,true);
        fs.delete(dir,true);
    }
    private static byte[] byteArrayForBloomKey(DataOutputBuffer buf){
        int cleanLength=buf.getLength();
        byte[] ba=buf.getData();
        if (cleanLength != ba.length) {
            ba=new byte[cleanLength];
            System.arraycopy(buf.getData(),0,ba,0,cleanLength);
        }
        return ba;
    }
    public static class Writer extends MapFile.Writer{
        private DynamicBloomFilter bloomFilter;
        private int numKeys;
        private int vectorSize;
        private Key bloomKey=new Key();
        private DataOutputBuffer buf=new DataOutputBuffer();
        private FileSystem fs;
        private Path dir;
        @Deprecated
        public Writer(Configuration conf, FileSystem fs, String dirName,
                      Class<? extends WritableComparable> keyClass,
                      Class<? extends Writable> valClass, SequenceFile.CompressionType compress,
                      CompressionCodec codec, Progressable progress)throws IOException{
            this(conf,new Path(dirName),keyClass(keyClass),valueClass(valClass),
                    compression(compress,codec),progressable(progress));
        }
        @Deprecated
        public Writer(Configuration conf, FileSystem fs, String dirName, Class<? extends WritableComparable> keyClass,
                      Class<? extends Writable> valClass, SequenceFile.CompressionType compress,
                      Progressable progress)throws IOException{
            this(conf,new Path(dirName),keyClass(keyClass),valueClass(valClass),
                    compression(compress),progressable(progress));
        }
        @Deprecated
        public Writer(Configuration conf, FileSystem fs, String dirName,
                      Class<? extends WritableComparable> keyClass,
                      Class valClass, SequenceFile.CompressionType compress)throws IOException{
            this(conf,new Path(dirName),keyClass(keyClass),valueClass(valClass),compression(compress));
        }

        @Deprecated
        public Writer(Configuration conf, FileSystem fs, String dirName, WritableComparator comparator,
                      Class valClass, SequenceFile.CompressionType compress,CompressionCodec codec,Progressable progress)throws IOException{
            this(conf,new Path(dirName),comparator(comparator),valueClass(valClass),compression(compress,codec),progressable(progress));
        }
        @Deprecated
        public Writer(Configuration conf, FileSystem fs, String dirName, WritableComparator comparator,
                      Class valClass, SequenceFile.CompressionType compress,CompressionCodec codec)throws IOException{
            this(conf,new Path(dirName),comparator(comparator),valueClass(valClass),compression(compress,codec));
        }
        @Deprecated
        public Writer(Configuration conf, FileSystem fs, String dirName, WritableComparator comparator,
                      Class valClass, SequenceFile.CompressionType compress)throws IOException{
            this(conf,new Path(dirName),comparator(comparator),valueClass(valClass),compression(compress));
        }
        @Deprecated
        public Writer(Configuration conf, FileSystem fs, String dirName, WritableComparator comparator,
                      Class valClass)throws IOException{
            this(conf,new Path(dirName),comparator(comparator),valueClass(valClass));
        }
        @Deprecated
        public Writer(Configuration conf, FileSystem fs, String dirName,
                      Class<? extends WritableComparable> keyClass,
                      Class valClass)throws IOException{
            this(conf,new Path(dirName),keyClass(keyClass),valueClass(valClass));
        }
        public Writer(Configuration conf,Path dir,SequenceFile.Writer writer.Option... options)throws IOException{
            super(conf,dir,options);
            this.fs=dir.getFileSystem(conf);
            this.dir=dir;
            initBloomFilter(conf);
        }
        private synchronized void initBloomFilter(Configuration conf){
            numKeys=conf.getInt(CommonConfigurationKeysPublic.IO_MAPFILE_BLOOM_SIZE_KEY,
                    CommonConfigurationKeysPublic.IO_MAPFILE_BLOOM_SIZE_DEFAULT);
            float errorRate=conf.getFloat(CommonConfigurationKeysPublic.IO_MAPFILE_BLOOM_ERROR_RATE_KEY,
                    CommonConfigurationKeysPublic.IO_MAPFILE_BLOOM_ERROR_RATE_DEFAULT);
            vectorSize=(int) Math.ceil((double) (-HASH_COUNT*numKeys)/
                    Math.log(1.0-Math.pow(errorRate,1.0/HASH_COUNT)));
            bloomFilter=new DynamicBloomFilter(vectorSize,HASH_COUNT,
                    Hash.getHashType(conf),numKeys);
        }

        @Override
        public synchronized void append(WritableComparable key,Writable val)throws IOException{
            super.append(key,val);
            buf.reset();
            key.writer(buf);
            bloomKey.set(byteArrayForBloomKey(buf),1.0);
            bloomFilter.add(bloomKey);
        }
        @Override
        public synchronized void close()throws IOException{
            super.close();
            DataOutputStream out=fs.create(new Path(dir,BLOOM_FILE_NAME),true);
            try {
                bloomFilter.write(out);
                out.flush();
                out.close();
                out=null;
            }finally {
                IOUtils.closeStream(out);
            }
        }
    }
    public static class Reader extends MapFile.Reader{
        private DynamicBloomFilter bloomFilter;
        private DataOutputBuffer buf=new DataOutputBuffer();
        private Key bloomKey=new Key();
        public Reader(Path dir,Configuration conf,SequenceFile.Reader.Option... options)throws IOException{
            super(dir,conf,options);
            initBloomFilter(dir,conf);
        }
        @Deprecated
        public Reader(FileSystem fs,String dirName,Configuration conf)throws IOException{
            this(new Path(dirName),conf);
        }
        @Deprecated
        public Reader(FileSystem fs,String dirName,WritableComparator comparator,
                      Configuration conf,boolean open)throws IOException{
            this(new Path(dirName),conf,comparator(comparator));
        }
        @Deprecated
        public Reader(FileSystem fs,String dirName,WritableComparator comparator,
                      Configuration conf)throws IOException{
            this(new Path(dirName),conf,comparator(comparator));
        }
        private void initBloomFilter(Path dirName,Configuration conf){
            DataInputStream in=null;
            try {
                FileSystem fs=dirName.getFileSystem(conf);
                in=fs.open(new Path(dirName,BLOOM_FILE_NAME));
                bloomFilter=new DynamicBloomFilter();
                bloomFilter.readFields(in);
                in.close();
                in=null;
            }catch (IOException e){
                LOG.warn("Can't open BloomFilter: "+e+" - fallback to MapFile.");
                bloomFilter=null;
            }finally {
                IOUtils.closeStream(in);
            }
        }
        public boolean probablyHashKey(WritableComparable key)throws IOException{
            if (bloomFilter == null) {
                return true;
            }
            buf.reset();
            key.writer(buf);
            bloomFilter.set(byteArrayForBloomKey(buf),1.0);
            return bloomFilter.membershipTest(bloomKey);
        }
        @Override
        public synchronized Writable get(WritableComparable key,Writable val)throws IOException{
            if (!probablyHashKey(key)) {
                return null;
            }
            return super.get(key,val);
        }
        public Filter getBloomFilter(){
            return bloomFilter;
        }
    }
}
