package org.apache.hadoop.util.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

public class HadoopScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor {
    private static final Logger LOG = LoggerFactory
            .getLogger(HadoopScheduledThreadPoolExecutor.class);

    public HadoopScheduledThreadPoolExecutor(int corePoolSize) {
        super(corePoolSize);
    }

    public HadoopScheduledThreadPoolExecutor(int corePoolSize,
                                             ThreadFactory threadFactory) {
        super(corePoolSize, threadFactory);
    }

    public HadoopScheduledThreadPoolExecutor(int corePoolSize,
                                             RejectedExecutionHandler handler) {
        super(corePoolSize, handler);
    }

    public HadoopScheduledThreadPoolExecutor(int corePoolSize,
                                             ThreadFactory threadFactory,
                                             RejectedExecutionHandler handler) {
        super(corePoolSize, threadFactory, handler);
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("beforeExecute in thread: " + Thread.currentThread()
                    .getName() + ", runnable type: " + r.getClass().getName());
        }
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        ExecutorHelper.logThrowableFromAfterExecute(r, t);
    }
}
