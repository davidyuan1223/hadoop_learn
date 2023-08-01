package org.apache.hadoop.fs;

import java.io.IOException;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/24
 **/
public interface RemoteIterator<E> {
    boolean hasNext() throws IOException;

    E next() throws IOException;
}
