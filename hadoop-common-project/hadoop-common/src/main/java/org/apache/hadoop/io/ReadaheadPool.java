package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadPoolExecutor;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ReadaheadPool {
    static final Logger LOG= LoggerFactory.getLogger(ReadaheadPool.class);
    private static final int POOL_SIZE=4;
    private static final int MAX_POOL_SIZE=16;
    private static final int CAPACITY=1024;
    private final ThreadPoolExecutor pool;
    private static ReadaheadPool instance;
    public static ReadaheadPool getInstance(){
        synchronized (ReadaheadPool.class){
            if (instance==null && NativeIO.isAvailable()){
                instance=new ReadaheadPool();
            }
            return instance;
        }
    }
}
