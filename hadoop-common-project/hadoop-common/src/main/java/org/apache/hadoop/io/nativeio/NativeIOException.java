package org.apache.hadoop.io.nativeio;

import org.apache.hadoop.util.Shell;

import java.io.IOException;

public class NativeIOException extends IOException {
    private static final long serialVersionUID=1L;
    private Errno errno;
    private int errorCode;
    public NativeIOException(String msg,Errno errno){
        super(msg);
        this.errno=errno;
        this.errorCode=0;
    }
    public NativeIOException(String msg,int errorCode){
        super(msg);
        this.errorCode=errorCode;
        this.errno=Errno.UNKNOWN;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public Errno getErrno() {
        return errno;
    }

    @Override
    public String toString() {
        if (Shell.WINDOWS){
            return errorCode+": "+super.getMessage();
        }else {
            return errno.toString()+": "+super.getMessage();
        }
    }
}
