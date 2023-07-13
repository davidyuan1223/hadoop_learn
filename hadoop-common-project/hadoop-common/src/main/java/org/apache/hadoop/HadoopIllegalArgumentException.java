package org.apache.hadoop;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class HadoopIllegalArgumentException extends IllegalArgumentException{
    private static final long serialVersionUID=1L;
    public HadoopIllegalArgumentException(final String message){super(message);}
}
