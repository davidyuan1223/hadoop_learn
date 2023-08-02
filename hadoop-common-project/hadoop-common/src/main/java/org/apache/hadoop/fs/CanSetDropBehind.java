package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.IOException;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface CanSetDropBehind {
    void setDropBehind(Boolean dropCache)throws IOException,UnsupportedOperationException;
}
