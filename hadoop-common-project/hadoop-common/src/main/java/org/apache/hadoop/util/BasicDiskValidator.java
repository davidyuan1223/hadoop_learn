package org.apache.hadoop.util;

import java.io.File;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/23
 **/
public class BasicDiskValidator implements DiskValidator {
    public static final String NAME="basic";

    @Override
    public void checkStatus(File dir) throws DiskErrorException {
        DiskChecker.checkDir(dir);
    }
}
