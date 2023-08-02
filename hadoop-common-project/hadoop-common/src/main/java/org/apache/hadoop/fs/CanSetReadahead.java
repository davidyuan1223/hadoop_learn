package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.IOException;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface CanSetReadahead {
    void setReadahead(Long readahead)throws IOException,UnsupportedOperationException;
}
