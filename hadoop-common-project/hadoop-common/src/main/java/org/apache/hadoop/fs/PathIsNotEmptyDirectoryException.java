package org.apache.hadoop.fs;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/25
 **/
public class PathIsNotEmptyDirectoryException extends PathExistsException {
    static final long serialVersionUID=0L;
    public PathIsNotEmptyDirectoryException(String path){
        super(path,"Is a directory");
    }
}
