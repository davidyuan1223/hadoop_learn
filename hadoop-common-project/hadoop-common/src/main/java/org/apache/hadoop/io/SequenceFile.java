package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import com.sun.xml.internal.ws.protocol.soap.VersionMismatchException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.util.Options;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.io.Closeable;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class SequenceFile {
    private static final Logger LOG=LoggerFactory.getLogger(SequenceFile.class);
    private SequenceFile(){}
    private static final byte BLOCK_COMPRESS_VERSION=(byte) 4;
    private static final byte CUSTOM_COMPRESS_VERSION=(byte) 5;
    private static final byte VERSION_WITH_METADATA=(byte) 6;
    private static byte[] VERSION=new byte[]{
            (byte) 'S',(byte) 'E',(byte) 'Q',VERSION_WITH_METADATA
    };
    private static final int SYNC_ESCAPE=1;
    private static final int SYNC_HASH_SIZE=16;
    private static final int SYNC_SIZE=4+SYNC_HASH_SIZE;
    private static final int SYNC_INTERVAL=5*1024*SYNC_SIZE;



    public static class Writer implements Closeable,Syncable,Flushable,StreamCapabilities{



        public static interface Option {}
        static class FileOption extends Options.PathOption implements Option{
            FileOption(Path path){
                super(path);
            }
        }
        @Deprecated
        private static class FileSystemOption implements Option{
            private FileSystem value;
            protected FileSystemOption(FileSystem value){
                this.value=value;
            }
            public FileSystem getValue(){
                return value;
            }
        }
        static class StreamOption extends Options.FSDataOutputStreamOption implements Option{
            StreamOption(FSDataOutputStream stream){
                super(stream);
            }
        }
        static class BufferSizeOption extends Options.IntegerOption implements Option{
            BufferSizeOption(int value){
                super(value);
            }
        }
        static class BlockSizeOption extends Options.LongOption implements Option{
            BlockSizeOption(long value){
                super(value);
            }
        }
        static class ReplicationOption extends Options.IntegerOption implements Option{
            ReplicationOption(int value){
                super(value);
            }
        }
        static class AppendIfExistsOption extends Options.BooleanOption implements Option{
            AppendIfExistsOption(boolean value){
                super(value);
            }
        }
        static class KeyClassOption extends Options.ClassOption implements Option{
            KeyClassOption(Class<?> value){
                super(value);
            }
        }
        static class ValueClassOption extends Options.ClassOption implements Option{
            ValueClassOption(Class<?> value){
                super(value);
            }
        }
        static class MetadataOption implements Option{
            private final Metadata value;
            MetadataOption(Metadata value){
                this.value=value;
            }
            Metadata getValue(){
                return value;
            }
        }
        static class ProgressableOption extends Options.ProgressableOption implements Option{
            ProgressableOption(Progressable value){
                super(value);
            }
        }
        private static class CompressionOption implements Option{
            private final CompressionType value;
            private final CompressionCodec codec;
            CompressionOption(CompressionType value){
                this(value,null);
            }
            CompressionOption(CompressionType value,CompressionCodec codec){
                this.value=value;
                this.codec=(CompressionType.NONE!=value&&null==codec)?
                        new DefaultCodec():codec;
            }

            public CompressionType getValue() {
                return value;
            }

            public CompressionCodec getCodec() {
                return codec;
            }
        }
        public static Option file(Path value){
            return new FileOption(value);
        }
        @Deprecated
        private static Option fileSystem(FileSystem fs){
            return new FileSystemOption(fs);
        }
        private static class SyncIntervalOption extends Options.IntegerOption implements Option{
            SyncIntervalOption(int val){
                super(val<0?SYNC_INTERVAL:val);
            }
        }

        public static Option bufferSize(int value){
            return new BufferSizeOption(value);
        }
        public static Option stream(FSDataOutputStream value){
            return new StreamOption(value);
        }
        public static Option replication(short value){
            return new ReplicationOption(value);
        }
        public static Option appendIfExists(boolean value){
            return new AppendIfExistsOption(value);
        }
        public static Option blockSize(long value){
            return new BlockSizeOption(value);
        }
        public static Option progressable(Progressable value){
            return new ProgressableOption(value);
        }
        public static Option keyClass(Class<?> value){
            return new KeyClassOption(value);
        }
        public static Option valueClass(Class<?> value){
            return new ValueClassOption(value);
        }
        public static Option metadata(Metadata value){
            return new MetadataOption(value);
        }
        public static Option compression(CompressionType value){
            return new CompressionOption(value);
        }
        public static Option compression(CompressionType value,CompressionCodec codec){
            return new CompressionOption(value,codec);
        }
        public static Option syncInterval(int value){
            return new SyncIntervalOption(value);
        }
        Writer(Configuration conf,Option... options)throws IOException{
            BlockSizeOption blockSizeOption = Options.getOption(BlockSizeOption.class, options);
            BufferSizeOption bufferSizeOption = Options.getOption(BufferSizeOption.class, options);
            ReplicationOption replicationOption = Options.getOption(ReplicationOption.class, options);
            ProgressableOption progressableOption = Options.getOption(ProgressableOption.class, options);
            FileOption fileOption = Options.getOption(FileOption.class, options);
            AppendIfExistsOption appendIfExistsOption = Options.getOption(AppendIfExistsOption.class, options);
            FileSystemOption fileSystemOption = Options.getOption(FileSystemOption.class, options);
            StreamOption streamOption = Options.getOption(StreamOption.class, options);
            KeyClassOption keyClassOption =
                    Options.getOption(KeyClassOption.class, options);
            ValueClassOption valueClassOption =
                    Options.getOption(ValueClassOption.class, options);
            MetadataOption metadataOption =
                    Options.getOption(MetadataOption.class, options);
            CompressionOption compressionTypeOption =
                    Options.getOption(CompressionOption.class, options);
            SyncIntervalOption syncIntervalOption =
                    Options.getOption(SyncIntervalOption.class, options);
            if ((fileOption == null) == (streamOption == null)) {
                throw new IllegalArgumentException("file or stream must be specified");
            }
            if (fileOption == null && (blockSizeOption != null ||
                    bufferSizeOption != null ||
                    replicationOption != null ||
                    progressableOption != null)) {
                throw new IllegalArgumentException("file modifier options not compatible with stream");
            }
            FSDataOutputStream out;
            boolean ownStream=fileOption!=null;
            if (ownStream) {
                Path p = fileOption.getValue();
                FileSystem fs;
                if (fileSystemOption != null) {
                    fs=fileSystemOption.getValue();
                }else {
                    fs=p.getFileSystem(conf);
                }
                int bufferSize = bufferSizeOption == null ? getBufferSize(conf) : bufferSizeOption.getValue();
                short replication=replicationOption!=null?fs.getDefaultReplication(p):(short) replicationOption.getValue();
                long blockSize=blockSizeOption==null?fs.getDefaultBlockSize(p):blockSizeOption.getValue();
                Progressable progress=progressableOption==null?null:progressableOption.getValue();
                if (appendIfExistsOption != null && appendIfExistsOption.getValue() && fs.exists(p)) {
                    SequenceFile.Reader reader=new Reader(conf,Reader.file(p),new Reader.onlyHeaderOption());
                    try {
                        if (keyClassOption.getValue() != reader.getKeyClass()
                                || valueClassOption.getValue() != reader.getValueClass()) {
                            throw new IllegalArgumentException("Key/value class provided does not match the file");
                        }
                        if (reader.getVersion()!=VERSION[3]){
                            throw new VersionMismatchException(VERSION[3],reader.getVersion());
                        }
                        if (metadataOption != null) {
                            LOG.info("MetaData Option is ignored during append");
                        }
                        metadataOption=(MetadataOption) Writer.metadata(reader.getMetadata());
                        new CompressionOption(reader.getCompressionType(),)
                    }
                }
            }
        }

    }
    public static class Metadata implements Writable{
        private TreeMap<Text,Text> theMetadata;
        public Metadata(){
            this(new TreeMap<>());
        }
        public Metadata(TreeMap<Text,Text> arg){
            if (arg == null) {
                this.theMetadata=new TreeMap<>();
            }else {
                this.theMetadata=arg;
            }
        }
        public Text get(Text name){
            return this.theMetadata.get(name);
        }
        public void set(Text name,Text value){
            this.theMetadata.put(name,value);
        }

        @Override
        public void writer(DataOutput out) throws IOException {
            out.write(this.theMetadata.size());
            Iterator<Map.Entry<Text, Text>> iterator = this.theMetadata.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Text, Text> entry = iterator.next();
                entry.getKey().writer(out);
                entry.getValue().writer(out);
            }
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            int sz = in.readInt();
            if (sz < 0) {
                throw new IOException("Invalid size: "+sz+" for file metadata object");
            }
            this.theMetadata=new TreeMap<>();
            for (int i = 0; i < sz; i++) {
                Text key=new Text();
                Text val=new Text();
                key.readFields(in);
                val.readFields(in);
                this.theMetadata.put(key,val);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (o == null) {
                return false;
            }
            if (o.getClass() != this.getClass()) {
                return false;
            }else {
                return equals((Metadata) o);
            }
        }
        public boolean equals(Metadata o){
            if (o == null) {
                return false;
            }
            if (this.theMetadata.size() != o.theMetadata.size()) {
                return false;
            }
            Iterator<Map.Entry<Text, Text>> it1 = this.theMetadata.entrySet().iterator();
            Iterator<Map.Entry<Text, Text>> it2 = o.theMetadata.entrySet().iterator();
            while (it1.hasNext() && it2.hasNext()) {
                Map.Entry<Text, Text> e1 = it1.next();
                Map.Entry<Text, Text> e2 = it2.next();
                if (!e1.getKey().equals(e2.getKey())) {
                    return false;
                }
                if (!e1.getValue().equals(e2.getValue())) {
                    return false;
                }
            }
            if (it1.hasNext() || it2.hasNext()) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            assert false : "hashCode not designed";
            return 42;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("size: ")
                    .append(this.theMetadata.size())
                    .append("\n");
            Iterator<Map.Entry<Text, Text>> it = this.theMetadata.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Text, Text> en = it.next();
                sb.append("\t")
                        .append(en.getKey().toString())
                        .append("\t")
                        .append(en.getValue().toString())
                        .append("\n");
            }
            return sb.toString();
        }
    }
    public enum CompressionType{
        NONE,
        RECORD,
        BLOCK
    }

    public static class Reader implements Closeable{
        private String filename;
        private FSDataInputStream in;
        private DataOutputBuffer outBuf=new DataOutputBuffer();
        private byte version;
        private String keyClassName;
        private String valClassName;
        private Class keyClass;
        private Class valClass;

        private CompressionCodec codec=null;
        private Metadata metadata=null;

        private byte[] sync=new byte[SYNC_HASH_SIZE];
        private byte[] syncCheck=new byte[SYNC_HASH_SIZE];
        private boolean syncSeen;

        private long headerEnd;
        private long end;
        private int keyLength;
        private int recordLength;

        private boolean decompress;
        private boolean blockCompressed;

        private Configuration conf;

        private int noBufferedRecords=0;
        private boolean lazyDecompress=true;
        private boolean valuesDecompressed=true;

        private int noBufferedKeys=0;
        private int noBufferedValues=0;

        private DataInputBuffer keyLenBuffer=null;
        private CompressionInputStream keyLenInFilter=null;
        private DataInputStream keyLenIn=null;
        private Decompressor keyLenDecompressor=null;
        private DataInputBuffer keyBuffer=null;
        private CompressionInputStream keyInFilter=null;
        private DataInputStream keyIn=null;
        private Decompressor keyDecompressor=null;

        private DataInputBuffer valLenBuffer=null;
        private CompressionInputStream valLenInFilter=null;
        private DataInputStream valLenIn=null;
        private Decompressor valLenDecompressor=null;
        private DataInputBuffer valBuffer=null;
        private CompressionInputStream valInFilter=null;
        private DataInputStream valIn=null;
        private Decompressor valDecompressor=null;

        private Deserializer keyDeserializer;
        private Deserializer valDeserializer;

        public static interface Option{}
        private static class FileOption extends Options.PathOption implements Option{
            private FileOption(Path value){
                super(value);
            }
        }
        private static class InputStreamOption extends Options.FSDataInputStreamOption implements Option{
            private InputStreamOption(FSDataInputStream value){
                super(value);
            }
        }
        private static class StartOption extends Options.LongOption implements Option{
            private StartOption(long value){
                super(value);
            }
        }
        private static class LengthOption extends Options.LongOption implements Option{
            private LengthOption(long value){
                super(value);
            }
        }
        private static class BufferSizeOption extends Options.IntegerOption implements Option{
            private BufferSizeOption(int value){
                super(value);
            }
        }
        private static class OnlyHeaderOption extends Options.BooleanOption implements Option{
            private OnlyHeaderOption(boolean value){
                super(value);
            }
        }
        public static Option file(Path value){
            return new FileOption(value);
        }
        public static Option stream(FSDataInputStream value){
            return new InputStreamOption(value);
        }
        public static Option start(long value){
            return new StartOption(value);
        }
        public static Option length(long value){
            return new LengthOption(value);
        }
        public static Option bufferSize(int value){
            return new BufferSizeOption(value);
        }
        public Reader(Configuration conf,Option... opts)throws IOException{
            FileOption fileOption = Options.getOption(FileOption.class, opts);
            InputStreamOption inputStreamOption = Options.getOption(InputStreamOption.class, opts);
            StartOption startOption = Options.getOption(StartOption.class, opts);
            LengthOption lengthOption = Options.getOption(LengthOption.class, opts);
            BufferSizeOption bufferSizeOption = Options.getOption(BufferSizeOption.class, opts);
            OnlyHeaderOption onlyHeaderOption = Options.getOption(OnlyHeaderOption.class, opts);
            if ((fileOption == null) == (inputStreamOption == null)) {
                throw new IllegalArgumentException("File or stream option must be specified");
            }
            if (fileOption == null && bufferSizeOption != null) {
                throw new IllegalArgumentException("buffer size can only be set when a file is specified");
            }
            Path filename=null;
            FSDataInputStream file;
            final long len;
            if (fileOption != null) {
                filename=fileOption.getValue();
                FileSystem fs=filename.getFileSystem(conf);
                int bufSize=bufferSizeOption==null?getBufferSize(conf):bufferSizeOption.getValue();
                len=null==lengthOption?fs.getFileStatus(filename).getLen():lengthOption.getValue();
                file=openFile(fs,filename,bufSize,len);
            }else {
                len=null==lengthOption?Long.MAX_VALUE:lengthOption.getValue();
                file=inputStreamOption.getValue();
            }
            long start=startOption==null?0:startOption.getValue();
            initialize(filename,file,start,len,conf,onlyHeaderOption!=null);
        }
        @Deprecated
        public Reader(FileSystem fs,Path file,Configuration conf)throws IOException{
            this(conf,file(fs.makeQualifed(file)));
        }
        @Deprecated
        public Reader(FSDataInputStream in,int bbufferSize,long start,long length,Configuration conf)throws IOException{
            this(conf,stream(in),start(start),length(length));
        }
        private void initialize(Path filename,FSDataInputStream in,
                                long start,long length,Configuration conf,
                                boolean tempReader)throws IOException{
            if (in == null) {
                throw new IllegalArgumentException("in==null");
            }
            this.filename=filename==null?"<unknown>":filename.toString();
            this.in=in;
            this.conf=conf;
            boolean succeed=false;
            try {
                seek(start);
                this.end=this.in.getPos()+length;
                if (end < length) {
                    end=Long.MAX_VALUE;
                }
                init(tempReader);
                succeed=true;
            }finally {
                if (!succeed) {
                    IOUtils.cleanupWithLogger(LOG,this.in);
                }
            }
        }
        protected FSDataInputStream openFile(FileSystem fs,Path file,int bufferSize,long length)throws IOException{
            FutureDataInputStreamBuilder builder=fs.openFile(file)
                    .opt(F)
        }
    }
}
