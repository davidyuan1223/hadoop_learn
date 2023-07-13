package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



@InterfaceAudience.Public
@InterfaceStability.Evolving
public class FileUtil {
    private static final Logger logger= LoggerFactory.getLogger(FileUtil.class);
    public static final int SYMLINK_NO_PRIVILEGE=2;
    private static final int BUFFER_SIZE=8_192;
    public static Path[] stat2Paths(FileStatus)
}
