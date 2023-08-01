package org.apache.hadoop.fs;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/25
 **/
public class PathNotFoundException extends PathIOException {
    static final long serialVersionUID=0L;
    public PathNotFoundException(String path){
        super(path,"No such file or directory;");
    }
    public PathNotFoundException(String path,Throwable cause){
        super(path,cause);
    }
    public PathNotFoundException(String path,String error){
        super(path,error);
    }
    public PathNotFoundException(String path,String error,Throwable cause){
        super(path,error,cause);
    }
}
