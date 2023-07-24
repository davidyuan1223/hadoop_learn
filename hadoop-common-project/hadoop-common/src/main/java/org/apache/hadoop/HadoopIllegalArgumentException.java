package org.apache.hadoop;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/23
 **/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class HadoopIllegalArgumentException extends IllegalArgumentException {
    private static final long serialVersionUID=1L;
    public HadoopIllegalArgumentException(final String msg){
        super(msg);
    }
}
