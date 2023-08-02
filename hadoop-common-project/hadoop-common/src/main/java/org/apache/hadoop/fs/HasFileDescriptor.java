package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.FileDescriptor;
import java.io.IOException;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface HasFileDescriptor {
    FileDescriptor getFileDescriptor()throws IOException;
}
