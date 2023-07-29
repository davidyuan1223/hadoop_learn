package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class ArrayFile extends MapFile{
    protected ArrayFile(){}
    public static class Writer extends MapFile.Writer{
        private LongWritable count=new LongWritable(0);
        public Writer(Configuration conf, FileSystem fs,String file,Class<? extends Writable> valClass)throws IOException {
            super(conf,new Path(file),keyClass(LongWritable.class),valueClass(valClass));
        }
        public Writer(Configuration conf, FileSystem fs, String file, Class<? extends Writable> valClass,
                      SequenceFile.CompressionType compress, Progressable progress)throws IOException{
            super(conf,new Path(file),
                    keyClass(LongWritable.class),
                    valueClass(valClass),
                    compression(compress),
                    progressable(progress));
        }
        public synchronized void append(Writable value)throws IOException{
            super.append(count,value);
            count.set(count.get()+1);
        }
    }
    public static class Reader extends MapFile.Reader{
        private LongWritable key=new LongWritable();
        public Reader(FileSystem fs,String  file,Configuration conf)throws IOException{
            super(new Path(file),conf);
        }
        public synchronized void seek(long n)throws IOException{
            key.set(n);
            seek(key);
        }
        public synchronized Writable next(Writable value)throws IOException{
            return next(key,value)?value:null;
        }
        public synchronized long key()throws IOException{
            return key.get();
        }
        public synchronized Writable get(long n,Writable value)throws IOException{
            key.set(n);
            return get(key,value);
        }
    }
}
