package org.apache.hadoop.fs;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/25
 **/
public class PathExistsException extends PathIOException {
    public PathExistsException(String path){super(path,"File exists");}
    public PathExistsException(String path,String error){
        super(path,error);
    }
}
