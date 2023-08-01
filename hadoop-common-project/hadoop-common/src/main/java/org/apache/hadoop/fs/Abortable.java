package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.IOException;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/24
 **/
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface Abortable {
    AbortableResult abort();

    interface AbortableResult{
        boolean alreadyClosed();
        IOException anyCleanupException();
    }
}
