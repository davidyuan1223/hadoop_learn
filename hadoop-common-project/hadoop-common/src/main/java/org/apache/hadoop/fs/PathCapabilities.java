package org.apache.hadoop.fs;

import java.io.IOException;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/25
 **/
public interface PathCapabilities {
    boolean hasPathCapability(Path path,String capability)throws IOException;
}
