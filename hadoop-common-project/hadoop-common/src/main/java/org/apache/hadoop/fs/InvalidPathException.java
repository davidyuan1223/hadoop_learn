package org.apache.hadoop.fs;

import org.apache.hadoop.HadoopIllegalArgumentException;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/29
 **/
public class InvalidPathException extends HadoopIllegalArgumentException {
    public InvalidPathException(final String s) {
        super("Invalid path name "+s);
    }
}
