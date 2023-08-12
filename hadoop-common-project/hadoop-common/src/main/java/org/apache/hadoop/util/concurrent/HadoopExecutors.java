package org.apache.hadoop.util.concurrent;

import org.slf4j.Logger;

import java.util.concurrent.*;

public class HadoopExecutors {
    public static ExecutorService newCachedThreadPool(ThreadFactory threadFactory){
        return new HadoopThreadPoolExecutor(0,Integer.MAX_VALUE,60L, TimeUnit.SECONDS,
                new SynchronousQueue<>(),threadFactory);
    }
    public static ExecutorService newFixedThreadPool(int nThread,ThreadFactory threadFactory){
        return new HadoopThreadPoolExecutor(nThread,nThread,0L,TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),threadFactory);
    }
    public static ExecutorService newFixedThreadPool(int nThread){
        return new HadoopThreadPoolExecutor(nThread,nThread,0L,TimeUnit.SECONDS,
                new LinkedBlockingQueue<>());
    }
    public static ExecutorService newSingleThreadExecutor() {
        return Executors.newSingleThreadExecutor();
    }

    //Executors.newSingleThreadExecutor has special semantics - for the
    // moment we'll delegate to it rather than implement the semantics here.
    public static ExecutorService newSingleThreadExecutor(ThreadFactory
                                                                  threadFactory) {
        return Executors.newSingleThreadExecutor(threadFactory);
    }

    public static ScheduledExecutorService newScheduledThreadPool(
            int corePoolSize) {
        return new HadoopScheduledThreadPoolExecutor(corePoolSize);
    }

    public static ScheduledExecutorService newScheduledThreadPool(
            int corePoolSize, ThreadFactory threadFactory) {
        return new HadoopScheduledThreadPoolExecutor(corePoolSize, threadFactory);
    }

    //Executors.newSingleThreadScheduledExecutor has special semantics - for the
    // moment we'll delegate to it rather than implement the semantics here
    public static ScheduledExecutorService newSingleThreadScheduledExecutor() {
        return Executors.newSingleThreadScheduledExecutor();
    }

    //Executors.newSingleThreadScheduledExecutor has special semantics - for the
    // moment we'll delegate to it rather than implement the semantics here
    public static ScheduledExecutorService newSingleThreadScheduledExecutor(
            ThreadFactory threadFactory) {
        return Executors.newSingleThreadScheduledExecutor(threadFactory);
    }
    public static void shutdown(ExecutorService executorService, Logger logger,long timeout,TimeUnit unit){
        if (executorService == null) {
            return;
        }
        try {
            executorService.shutdown();
            logger.debug("Gracefully shutting down executor service {}. Waiting max {} {}",executorService,timeout,unit);
            if (!executorService.awaitTermination(timeout, unit)) {
                logger.debug(
                        "Executor service has not shutdown yet. Forcing. "
                                + "Will wait up to an additional {} {} for shutdown",
                        timeout, unit);
                executorService.shutdownNow();
            }
            if (executorService.awaitTermination(timeout, unit)) {
                logger.debug("Succesfully shutdown executor service");
            } else {
                logger.error("Unable to shutdown executor service after timeout {} {}",
                        (2 * timeout), unit);
            }
        }catch (InterruptedException e) {
            logger.error("Interrupted while attempting to shutdown", e);
            executorService.shutdownNow();
        } catch (Exception e) {
            logger.warn("Exception closing executor service {}", e.getMessage());
            logger.debug("Exception closing executor service", e);
            throw e;
        }
    }
    private HadoopExecutors() { }
}

