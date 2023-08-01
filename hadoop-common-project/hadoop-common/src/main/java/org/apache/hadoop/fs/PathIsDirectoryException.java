package org.apache.hadoop.fs;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/25
 **/
public class PathIsDirectoryException extends PathExistsException {
    public PathIsDirectoryException(String path){
        super(path,"Directory is not empty");
    }
}
