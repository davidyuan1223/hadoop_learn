package org.apache.hadoop.util;

import com.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.statistics.DurationTracker;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;
import org.apache.hadoop.fs.statistics.IOStatisticsSupport;
import org.apache.hadoop.fs.statistics.StoreStatisticNames;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ForwardingExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Futures;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

@SuppressWarnings("NullableProblems")
@InterfaceAudience.Private
public class SemaphoredDelegatingExecutor extends ForwardingExecutorService {
    private final Semaphore queueingPermits;
    private final ExecutorService executorDelegate;
    private final int permitCount;
    private final DurationTrackerFactory trackerFactory;

    public SemaphoredDelegatingExecutor(ExecutorService executorDelegate,
                                        int permitCount,boolean fair,
                                        DurationTrackerFactory trackerFactory){
        this.permitCount=permitCount;
        queueingPermits=new Semaphore(permitCount,fair);
        this.executorDelegate= Objects.requireNonNull(executorDelegate);
        this.trackerFactory=trackerFactory!=null
                ?trackerFactory
                : IOStatisticsSupport.stubDurationTrackerFactory();
    }

    public SemaphoredDelegatingExecutor(ExecutorService executorDelegate,
                                        int permitCount,boolean fair){
        this(executorDelegate,permitCount,fair,null);
    }

    @Override
    protected ExecutorService delegate() {
        return executorDelegate;
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        try(DurationTracker ignored=trackerFactory.trackDuration(StoreStatisticNames.ACTION_EXECUTOR_ACQUIRED)){
            queueingPermits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Futures.immediateFailedFuture(e);
        }
        return super.submit(new CallableWithPermitRelease<>(task));
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        try(DurationTracker ignored=trackerFactory.trackDuration(StoreStatisticNames.ACTION_EXECUTOR_ACQUIRED)){
            queueingPermits.acquire();
        }catch (InterruptedException e){
            Thread.currentThread().interrupt();
            return Futures.immediateFailedFuture(e);
        }
        return super.submit(new RunnableWithPermitRelease(task),result);
    }

    @Override
    public Future<?> submit(Runnable task) {
        try(DurationTracker ignored = trackerFactory.trackDuration(StoreStatisticNames.ACTION_EXECUTOR_ACQUIRED)){
            queueingPermits.acquire();
        }catch (InterruptedException e){
            Thread.currentThread().interrupt();
            return Futures.immediateFailedFuture(e);
        }
        return super.submit(new RunnableWithPermitRelease(task));
    }

    @Override
    public void execute(Runnable command) {
        try(final DurationTracker ignored = trackerFactory.trackDuration(StoreStatisticNames.ACTION_EXECUTOR_ACQUIRED)){
            queueingPermits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        super.execute(new RunnableWithPermitRelease(command));
    }

    public int getAvailablePermits(){
        return queueingPermits.availablePermits();
    }
    public int getWaitingCount(){
        return queueingPermits.getQueueLength();
    }
    public int getPermitCount(){
        return permitCount;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(
                "SemaphoredDelegatingExecutor{");
        sb.append("permitCount=").append(getPermitCount())
                .append(", available=").append(getAvailablePermits())
                .append(", waiting=").append(getWaitingCount())
                .append('}');
        return sb.toString();
    }

    class CallableWithPermitRelease<T> implements Callable<T>{
        private Callable<T> delegate;
        CallableWithPermitRelease(Callable<T> delegatee){
            this.delegate=delegatee;
        }

        @Override
        public T call() throws Exception {
            try {
                return delegate.call();
            }finally {
                queueingPermits.release();
            }
        }
    }
    class RunnableWithPermitRelease implements Runnable{
        private Runnable delegate;
        RunnableWithPermitRelease(Runnable delegate){
            this.delegate=delegate;
        }

        @Override
        public void run() {
            try {
                delegate.run();
            }finally {
                queueingPermits.release();
            }
        }
    }
}
