package org.apache.hadoop.fs;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public interface FutureDataInputStreamBuilder
extends FSBuilder<CompletableFuture<FSDataInputStream>,FutureDataInputStreamBuilder> {
    @Override
    CompletableFuture<FSDataInputStream> build() throws IllegalArgumentException, UnsupportedOperationException, IOException;
    default FutureDataInputStreamBuilder withFileStatus(@Nullable FileStatus status){
        return this;
    }
}
