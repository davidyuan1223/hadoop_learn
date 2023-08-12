package org.apache.hadoop.util.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public final class ExecutorHelper {
    private static final Logger LOG= LoggerFactory.getLogger(ExecutorHelper.class);
    static void logThrowableFromAfterExecute(Runnable r,Throwable t){
        if (LOG.isDebugEnabled()) {
            LOG.debug("afterExecute in thread: "+Thread.currentThread().getName()
            +", runnable type: "+r.getClass().getName());
        }
        if (t == null && r instanceof Future<?> && ((Future<?>) r).isDone()) {
            try {
                ((Future<?>)r).get();
            }catch (ExecutionException e){
                LOG.debug("Execution exception when running task in {}",Thread.currentThread().getName());
                t=e.getCause();
            }catch (InterruptedException e){
                LOG.debug("Thread ( {} ) interrupted: ",Thread.currentThread(),e);
                Thread.currentThread().interrupt();
            }catch (Throwable throwable){
                t=throwable;
            }
        }
        if (t != null) {
            LOG.warn("Caught exception in thread {} + : ",Thread.currentThread().getName(),t);
        }
    }

    private ExecutorHelper(){}
}
