package org.apache.hadoop.fs.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

import java.util.Set;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/30
 **/
public class OpenFileParameters {
    private Set<String > mandatoryKeys;
    private Set<String > optionalKeys;
    private Configuration conf;
    private int bufferSize;
    private FileStatus status;
    public OpenFileParameters(){}

}
