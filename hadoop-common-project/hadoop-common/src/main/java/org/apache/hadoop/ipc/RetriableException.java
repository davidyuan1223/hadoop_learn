package org.apache.hadoop.ipc;

import com.apache.hadoop.classification.InterfaceStability;

import java.io.IOException;
@InterfaceStability.Evolving
public class RetriableException extends IOException {
    private static final long serialVersionUID=1915561725516487301L;
    public RetriableException(Exception e){super(e);}
    public RetriableException(String msg){super(msg);}
}
