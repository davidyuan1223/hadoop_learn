package org.apache.hadoop.security;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.IOException;
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AccessControlException extends IOException {
    private static final long serialVersionUID=1L;

    public AccessControlException(){
        super("Permission denied");
    }
    public AccessControlException(String s){
        super(s);
    }
    public AccessControlException(Throwable cause){
        super(cause);
    }
}
