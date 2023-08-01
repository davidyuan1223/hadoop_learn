package org.apache.hadoop.fs;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/24
 **/
public class PathAccessDeniedException extends PathIOException {
    static final long serialVersionUID=0L;
    public PathAccessDeniedException(String path){
        super(path,"Permission denied");
    }
    public PathAccessDeniedException(String path,Throwable cause){
        super(path,cause);
    }
    public PathAccessDeniedException(String path,String error,Throwable cause){
        super(path,error,cause);
    }
}
