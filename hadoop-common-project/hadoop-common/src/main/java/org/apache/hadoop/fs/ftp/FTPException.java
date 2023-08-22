package org.apache.hadoop.fs.ftp;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class FTPException extends RuntimeException{
    private static final long serialVersionUID=1L;
    public FTPException(String message){
        super(message);
    }
    public FTPException(Throwable t){
        super(t);
    }
    public FTPException(String message,Throwable t){
        super(message,t);
    }
}
