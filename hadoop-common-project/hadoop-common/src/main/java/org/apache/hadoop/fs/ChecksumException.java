package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.IOException;
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ChecksumException extends IOException {
    private static final long serialVersionUID=1L;
    private long pos;
    public ChecksumException(String description,long pos){
        super(description);
        this.pos=pos;
    }

    public long getPos() {
        return pos;
    }
}
