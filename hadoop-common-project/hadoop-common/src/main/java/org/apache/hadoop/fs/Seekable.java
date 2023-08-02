package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.IOException;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface Seekable {
    void seek(long pos)throws IOException;

    long getPos() throws IOException;

    @InterfaceAudience.Private
    boolean seekToNewSource(long targetPos)throws IOException;
}
