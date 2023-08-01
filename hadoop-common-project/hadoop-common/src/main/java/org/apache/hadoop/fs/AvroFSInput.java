package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.avro.file.SeekableInput;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL;
import static org.apache.hadoop.util.functional.FutureIO.awaitFuture;
import java.io.Closeable;
import java.io.IOException;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/30
 **/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class AvroFSInput implements Closeable, SeekableInput {
    private final FSDataInputStream stream;
    private final long len;

    public AvroFSInput(final FSDataInputStream in,final long len){
        this.stream=in;
        this.len=len;
    }

    public AvroFSInput(final FileContext fc,final Path p){
        FileStatus status=fc.getFileStatus(p);
        this.len=status.getLen();
        this.stream=awaitFuture(fc.openFile(p)
        .opt(FS_OPTION_OPENFILE_READ_POLICY,
                FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL)
                .withFileStatus(status)
                .build());
    }

    @Override
    public long length() throws IOException {
        return len;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return stream.read(b,off,len);
    }

    @Override
    public void seek(long p) throws IOException {
        stream.seek();
    }

    @Override
    public long tell() throws IOException {
        return stream.getPos();
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }
}
