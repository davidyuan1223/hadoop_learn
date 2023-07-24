package org.apache.hadoop.util;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.File;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/23
 **/
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface DiskValidator {
    void checkStatus(File dir)throws DiskChecker.DiskErrorException;
}
