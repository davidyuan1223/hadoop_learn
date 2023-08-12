package org.apache.hadoop.util;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import com.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ShutdownHookManager {
    private static final ShutdownHookManager MGR=new ShutdownHookManager();
    private static final Logger LOG= LoggerFactory.getLogger(ShutdownHookManager.class);
    public static final long TIMEOUT_MINUTE=1;
    public static final TimeUnit TIME_UNIT_DEFAULT=TimeUnit.SECONDS;
    private static final ExecutorService EXECUTOR= HadoopExecutors.
            newSingleThreadExecutor(new ThreadFactoryBuilder()
    .setDaemon(true)
    .setNameFormat("shutdown-hook-%01d")
    .build());
    private final Set<HookEntry> hooks= Collections.synchronizedSet(new HashSet<>());
    private AtomicBoolean shutdownIfProgress=new AtomicBoolean(false);
    static {
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(){
                @Override
                public void run() {
                    if (MGR.shutdownIfProgress.getAndSet(true)) {
                        LOG.info("Shutdown process invoked a second time: ignoring");
                        return;
                    }
                    long started = System.currentTimeMillis();
                    int timeoutCount = MGR.executeShutdown();
                    long ended = System.currentTimeMillis();
                    LOG.debug(String.format(
                            "Completed shutdown in %.3f seconds; Timeouts: %d",
                            (ended-started)/1000.0,timeoutCount
                    ));
                    shutdownExecutor(new Configuration());
                }
            });
        }catch (IllegalStateException e){
            LOG.warn("Failed to add the ShutdownHook",e);
        }
    }

    @InterfaceAudience.Private
    @VisibleForTesting
    int executeShutdown(){
        int timeouts=0;
        for (HookEntry entry : getShutdownHookInOrder()) {
            Future<?> future = EXECUTOR.submit(entry.getHook());
            try {
                future.get(entry.getTimeout(),entry.getTimeUnit());
            }catch (TimeoutException e){
                timeouts++;
                future.cancel(true);
                LOG.warn("ShutdownHook '"+entry.getHook().getClass().getSimpleName()
                +"' timeout, "+e.toString(),e);
            }catch (Throwable t){
                LOG.warn("ShutdownHook '" + entry.getHook().getClass().
                        getSimpleName() + "' failed, " + t.toString(), t);
            }
        }
        return timeouts;
    }

    private static void shutdownExecutor(final Configuration conf){
        try {
            EXECUTOR.shutdown();
            long shutdownTimeout=getShutdownTimeout(conf);
            if (!EXECUTOR.awaitTermination(shutdownTimeout, TIME_UNIT_DEFAULT)) {
                LOG.error("ShutdownHookManager shutdown forcefully after {} seconds.",shutdownTimeout);
                EXECUTOR.shutdownNow();
            }
            LOG.debug("ShutdownHookManager completed shutdown.");
        }catch (InterruptedException e){
            LOG.error("ShutdownHookManager interrupted while waiting for termination.",e);
            EXECUTOR.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
    @InterfaceAudience.Public
    public static ShutdownHookManager get(){
        return MGR;
    }
    @InterfaceAudience.Private
    @VisibleForTesting
    static long getShutdownTimeout(Configuration conf){
        long duration = conf.getTimeDuration(CommonConfigurationKeys.SERVICE_SHUTDOWN_TIMEOUT, CommonConfigurationKeys.SERVICE_SHUTDOWN_TIMEOUT_DEFAULT, TIME_UNIT_DEFAULT);
        if (duration < TIMEOUT_MINUTE) {
            duration=TIMEOUT_MINUTE;
        }
        return duration;
    }
    @VisibleForTesting
    @InterfaceAudience.Private
    ShutdownHookManager(){}

    @InterfaceAudience.Public
    @InterfaceStability.Stable
    public void addShutdownHook(Runnable shutdownHook,int priority){
        if (shutdownHook == null) {
            throw new IllegalArgumentException("shutdownHook cannot be NULL");
        }
        if (shutdownIfProgress.get()) {
            throw new IllegalArgumentException("Shutdown in progress,cannot add a shutdownHook");
        }
        hooks.add(new HookEntry(shutdownHook,priority));
    }
    @InterfaceAudience.Public
    @InterfaceStability.Stable
    public void addShutdownHook(Runnable shutdownHook, int priority, long timeout,
                                TimeUnit unit) {
        if (shutdownHook == null) {
            throw new IllegalArgumentException("shutdownHook cannot be NULL");
        }
        if (shutdownIfProgress.get()) {
            throw new IllegalStateException("Shutdown in progress, cannot add a " +
                    "shutdownHook");
        }
        hooks.add(new HookEntry(shutdownHook, priority, timeout, unit));
    }

    /**
     * Removes a shutdownHook.
     *
     * @param shutdownHook shutdownHook to remove.
     * @return TRUE if the shutdownHook was registered and removed,
     * FALSE otherwise.
     */
    @InterfaceAudience.Public
    @InterfaceStability.Stable
    public boolean removeShutdownHook(Runnable shutdownHook) {
        if (shutdownIfProgress.get()) {
            throw new IllegalStateException("Shutdown in progress, cannot remove a " +
                    "shutdownHook");
        }
        // hooks are only == by runnable
        return hooks.remove(new HookEntry(shutdownHook, 0, TIMEOUT_MINUTE,
                TIME_UNIT_DEFAULT));
    }

    /**
     * Indicates if a shutdownHook is registered or not.
     *
     * @param shutdownHook shutdownHook to check if registered.
     * @return TRUE/FALSE depending if the shutdownHook is is registered.
     */
    @InterfaceAudience.Public
    @InterfaceStability.Stable
    public boolean hasShutdownHook(Runnable shutdownHook) {
        return hooks.contains(new HookEntry(shutdownHook, 0, TIMEOUT_MINUTE,
                TIME_UNIT_DEFAULT));
    }

    /**
     * Indicates if shutdown is in progress or not.
     *
     * @return TRUE if the shutdown is in progress, otherwise FALSE.
     */
    @InterfaceAudience.Public
    @InterfaceStability.Stable
    public boolean isShutdownInProgress() {
        return shutdownIfProgress.get();
    }

    /**
     * clear all registered shutdownHooks.
     */
    @InterfaceAudience.Public
    @InterfaceStability.Stable
    public void clearShutdownHooks() {
        hooks.clear();
    }
    @InterfaceAudience.Private
    @VisibleForTesting
    List<HookEntry> getShutdownHookInOrder(){
        List<HookEntry> list;
        synchronized (hooks){
            list=new ArrayList<>(hooks);
        }
        Collections.sort(list, new Comparator<HookEntry>() {
            @Override
            public int compare(HookEntry o1, HookEntry o2) {
                return o2.priority-o1.priority;
            }
        });
        return list;
    }

    @InterfaceAudience.Private
    @VisibleForTesting
    static class HookEntry{
        private final Runnable hook;
        private final int priority;
        private final long timeout;
        private final TimeUnit unit;

        HookEntry(Runnable hook,int priority){
            this(hook,priority,getShutdownTimeout(new Configuration()),TIME_UNIT_DEFAULT);
        }
        HookEntry(Runnable hook, int priority,long timeout,TimeUnit unit){
            this.hook=hook;
            this.priority=priority;
            this.timeout=timeout;
            this.unit=unit;
        }
        @Override
        public int hashCode() {
            return hook.hashCode();
        }
        @Override
        public boolean equals(Object obj) {
            boolean eq = false;
            if (obj != null) {
                if (obj instanceof HookEntry) {
                    eq = (hook == ((HookEntry)obj).hook);
                }
            }
            return eq;
        }

        Runnable getHook() {
            return hook;
        }

        int getPriority() {
            return priority;
        }

        long getTimeout() {
            return timeout;
        }

        TimeUnit getTimeUnit() {
            return unit;
        }
    }
}
