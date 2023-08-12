package org.apache.hadoop.util.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public final class HadoopThreadPoolExecutor extends ThreadPoolExecutor {
    private static final Logger LOG= LoggerFactory.getLogger(HadoopThreadPoolExecutor.class);
    public HadoopThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
                                    BlockingQueue<Runnable> workQueue){
        super(corePoolSize,maximumPoolSize,keepAliveTime,unit,workQueue);
    }
    public HadoopThreadPoolExecutor(int corePoolSize,
                                    int maximumPoolSize,
                                    long keepAliveTime,
                                    TimeUnit unit,
                                    BlockingQueue<Runnable> workQueue,
                                    ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
                threadFactory);
    }

    public HadoopThreadPoolExecutor(int corePoolSize,
                                    int maximumPoolSize,
                                    long keepAliveTime,
                                    TimeUnit unit,
                                    BlockingQueue<Runnable> workQueue,
                                    RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
                handler);
    }

    public HadoopThreadPoolExecutor(int corePoolSize,
                                    int maximumPoolSize,
                                    long keepAliveTime,
                                    TimeUnit unit,
                                    BlockingQueue<Runnable> workQueue,
                                    ThreadFactory threadFactory,
                                    RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
                threadFactory, handler);
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("beforeExecute in thread: "+Thread.currentThread().getName()
            +", runnable type: "+r.getClass().getName());
        }
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        ExecutorHelper.logThrowableFromAfterExecute(r,t);
    }
}
