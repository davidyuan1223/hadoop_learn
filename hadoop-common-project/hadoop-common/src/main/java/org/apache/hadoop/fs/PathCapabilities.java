package org.apache.hadoop.fs;

import java.io.IOException;

public interface PathCapabilities {
    boolean hasPathCapability(Path path,String capability)throws IOException;
}
