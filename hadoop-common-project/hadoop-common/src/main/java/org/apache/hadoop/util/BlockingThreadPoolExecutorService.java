package org.apache.hadoop.util;

import com.apache.hadoop.classification.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@InterfaceAudience.Private
public final class BlockingThreadPoolExecutorService extends SemaphoredDelegatingExecutor{
    private static final Logger LOG= LoggerFactory.getLogger(BlockingThreadPoolExecutorService.class);
    private static final AtomicInteger POOLNUMBER=new AtomicInteger(1);
    private final ThreadPoolExecutor eventProcessingExecutor;

    static ThreadFactory getNameThreadFactory(final String prefix){
        SecurityManager s = System.getSecurityManager();
        final ThreadGroup threadGroup=(s!=null)?s.getThreadGroup():Thread.currentThread().getThreadGroup();
        return new ThreadFactory() {
            private final AtomicInteger threadNumber=new AtomicInteger(1);
            private final int poolNum=POOLNUMBER.getAndIncrement();
            private final ThreadGroup group=threadGroup;
            @Override
            public Thread newThread(Runnable r) {
                final String name=prefix+"-pool"+poolNum+"-t"+threadNumber.getAndIncrement();
                return new Thread(group,r,name);
            }
        };
    }
    public static ThreadFactory newDaemonThreadFactory(final String prefix){
        final ThreadFactory namedFactory=getNameThreadFactory(prefix);
        return new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = namedFactory.newThread(r);
                if (!t.isDaemon()) {
                    t.setDaemon(true);
                }
                if (t.getPriority() != Thread.NORM_PRIORITY) {
                    t.setPriority(Thread.NORM_PRIORITY);
                }
                return t;
            }
        };
    }
    private BlockingThreadPoolExecutorService(int permitCount,ThreadPoolExecutor eventProcessingExecutor){
        super(eventProcessingExecutor,permitCount,false);
        this.eventProcessingExecutor=eventProcessingExecutor;
    }

    public static BlockingThreadPoolExecutorService newInstance(int activeTasks, int waitingTasks,
                                                                long keepAliveTime, TimeUnit unit,String prefixName){
        final BlockingQueue<Runnable> workQueue=new LinkedBlockingDeque<>(waitingTasks+activeTasks);
        ThreadPoolExecutor eventProcessingExecutor=
                new ThreadPoolExecutor(activeTasks, activeTasks, keepAliveTime, unit, workQueue, newDaemonThreadFactory(prefixName),
                        new RejectedExecutionHandler() {
                            @Override
                            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                                LOG.error("Could not submit task or executor {}",executor.toString());
                            }
                        });
        eventProcessingExecutor.allowCoreThreadTimeOut(true);
        return new BlockingThreadPoolExecutorService(waitingTasks+activeTasks,eventProcessingExecutor);
    }

    int getActiveCount(){
        return eventProcessingExecutor.getActiveCount();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(
                "BlockingThreadPoolExecutorService{");
        sb.append(super.toString())
                .append(", activeCount=").append(getActiveCount())
                .append('}');
        return sb.toString();
    }
}
