package org.apache.hadoop.fs.impl;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class FutureDataInputStreamBuilderImpl
extends AbstractFSBuilderImpl<CompletableFuture<FSDataInputStream>, FutureDataInputStreamBuilder>
implements FutureDataInputStreamBuilder{
    private final FileSystem fileSystem;
    private int bufferSize;
    private FileStatus status;

    protected FutureDataInputStreamBuilderImpl(@Nonnull FileContext fc,
                                               @Nonnull Path path)throws IOException{
        super(Objects.requireNonNull(path,"path"));
        Objects.requireNonNull(fc,"file context");
        this.fileSystem=null;
        bufferSize=CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_DEFAULT;
    }

    protected FutureDataInputStreamBuilderImpl(@Nonnull FileSystem fileSystem,
                                               @Nonnull Path path){
        super(Objects.requireNonNull(path,"path"));
        this.fileSystem=Objects.requireNonNull(fileSystem,"fileSystem");
        initFromFS();
    }

    public FutureDataInputStreamBuilderImpl(@Nonnull FileSystem fileSystem,@Nonnull PathHandler pathHandler){
        super(pathHandler);
        this.fileSystem=fileSystem;
        initFromFS();
    }

    private void initFromFS(){
        bufferSize=fileSystem.getConf().getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY,
                CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_DEFAULT);
    }

    protected FileSystem getFS(){
        Objects.requireNonNull(fileSystem,"fileSystem");
        return fileSystem;
    }

    protected int getBufferSize() {
        return bufferSize;
    }
    public FutureDataInputStreamBuilder bufferSize(int bufSize) {
        bufferSize = bufSize;
        return getThisBuilder();
    }

    public FutureDataInputStreamBuilder builder() {
        return getThisBuilder();
    }

    @Override
    public FutureDataInputStreamBuilder getThisBuilder() {
        return this;
    }

    @Override
    public FutureDataInputStreamBuilder withFileStatus(
            @Nullable FileStatus st) {
        this.status = st;
        return this;
    }
    protected FileStatus getStatus() {
        return status;
    }
}
