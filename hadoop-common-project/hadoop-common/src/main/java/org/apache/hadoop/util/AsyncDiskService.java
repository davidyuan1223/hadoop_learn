package org.apache.hadoop.util;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/23
 **/
@InterfaceAudience.LimitedPrivate({"HDFS","MapReduce"})
@InterfaceStability.Unstable
public class AsyncDiskService {
    private static final Logger log= LoggerFactory.getLogger(AsyncDiskService.class);
    private static final int CORE_THREADS_PER_VOLUME=1;
    private static final int MAXIMUM_THREADS_PER_VOLUME=4;
    private static final long THREADS_KEEP_ALIVE_SECOND=60;
    private final ThreadGroup threadGroup=new ThreadGroup("async disk service");
    private ThreadFactory threadFactory;
    private HashMap<String , ThreadPoolExecutor> executors=new HashMap<>();

    public AsyncDiskService(String[] volumes){
        threadFactory= r -> new Thread(threadGroup,r);;
        for (int i = 0; i < volumes.length; i++) {
            ThreadPoolExecutor executor=new ThreadPoolExecutor(
                    CORE_THREADS_PER_VOLUME,MAXIMUM_THREADS_PER_VOLUME,
                    THREADS_KEEP_ALIVE_SECOND, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(),threadFactory
            );
            executor.allowCoreThreadTimeOut(true);
            executors.put(volumes[i],executor);
        }
    }
    public synchronized void execute(String root,Runnable task){
        ThreadPoolExecutor executor = executors.get(root);
        if (executor == null) {
            throw new RuntimeException("Cannot find root "+root+" for execution of task "+task);
        }else {
            executor.execute(task);
        }
    }
    public synchronized void shutdown(){
        log.info("Shutting down all AsyncDiskService threads...");
        for (Map.Entry<String, ThreadPoolExecutor> e : executors.entrySet()) {
            e.getValue().shutdown();
        }
    }
    public synchronized boolean awaitTermination(long milliseconds){
        long end=Time.now()+milliseconds;
        for (Map.Entry<String, ThreadPoolExecutor> e : executors.entrySet()) {
            ThreadPoolExecutor executor = e.getValue();
            if (!executor.awaitTermination(Math.max(end - Time.now(), 0), TimeUnit.MILLISECONDS)) {
                log.warn("AsyncDiskService awaitTermination timeout");
                return false;
            }
        }
        log.info("All AsyncDiskService threads are terminated");
        return true;
    }
    public synchronized List<Runnable> shutdownNow(){
        log.info("Shutting down all AsyncDiskService threads immediately...");
        List<Runnable> list=new ArrayList<>();
        for (Map.Entry<String, ThreadPoolExecutor> e : executors.entrySet()) {
            list.addAll(e.getValue().shutdownNow());
        }
        return list;
    }
}
