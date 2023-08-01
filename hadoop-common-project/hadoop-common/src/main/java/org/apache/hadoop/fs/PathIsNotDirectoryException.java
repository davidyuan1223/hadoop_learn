package org.apache.hadoop.fs;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/25
 **/
public class PathIsNotDirectoryException extends PathExistsException {
    static final long serialVersionUID=0L;
    public PathIsNotDirectoryException(String path){
        super(path,"Is a directory");
    }
}
