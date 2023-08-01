package org.apache.hadoop.ipc;

import com.apache.hadoop.classification.InterfaceStability;

import java.io.IOException;
@InterfaceStability.Evolving
public class StandbyException extends IOException {
    static final long serialVersionUID=0x12308AD010L;
    public StandbyException(String msg){
        super(msg);
    }
}
