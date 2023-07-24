package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/23
 **/
@Deprecated
@InterfaceAudience.LimitedPrivate({"HDFS","MapReduce"})
@InterfaceStability.Stable
public class UTF8 implements WritableComparable<UTF8>{
    private static final Logger LOG= LoggerFactory.getLogger(UTF8.class);
    private static final DataInputBuffer IBUF=new DataInputBuffer();
    private static final ThreadLocal<DataOutopyu>
}
