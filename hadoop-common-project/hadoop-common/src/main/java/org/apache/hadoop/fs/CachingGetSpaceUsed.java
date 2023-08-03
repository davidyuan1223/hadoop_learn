package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import com.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@InterfaceAudience.LimitedPrivate({"HDFS","MapReduce"})
@InterfaceStability.Evolving
public abstract class CachingGetSpaceUsed implements Closeable,GetSpaceUsed {
    static final Logger LOG= LoggerFactory.getLogger(CachingGetSpaceUsed.class);
    protected final AtomicLong used=new AtomicLong();
    private final AtomicBoolean running=new AtomicBoolean(true);
    private final long refreshInterval;
    private final long jitter;
    private final String dirPath;
    private Thread refreshUsed;
    private boolean shouldFirstRefresh;

    public CachingGetSpaceUsed(CachingGetSpaceUsed.Builder builder)throws IOException {
        this(builder.getPath(),builder.getInterval(),builder.getJitter(),builder.getInitialUsed());
    }
    CachingGetSpaceUsed(File path,long interval,long jitter,long initialUsed)throws IOException{
        this.dirPath=path.getCanonicalPath();
        this.refreshInterval=interval;
        this.jitter=jitter;
        this.used.set(initialUsed);
        this.shouldFirstRefresh=true;
    }
    void init(){
        if (used.get() < 0) {
            used.set(0);
            if (!shouldFirstRefresh) {
                initRefreshThread(true);
                return;
            }
            refresh();
        }
        initRefreshThread(false);
    }

    private void initRefreshThread(boolean runImmediately){
        if (refreshInterval > 0) {
            refreshUsed=new Thread(new RefreshThread(this,runImmediately),"refreshUsed-"+dirPath);
            refreshUsed.setDaemon(true);
            refreshUsed.start();
        }else {
            running.set(false);
            refreshUsed=null;
        }
    }
    protected abstract void refresh();
    protected void setShouldFirstRefresh(boolean shouldFirstRefresh){
        this.shouldFirstRefresh=shouldFirstRefresh;
    }

    @Override
    public long getUsed() {
        return Math.max(used.get(),0);
    }

    public String getDirPath() {
        return dirPath;
    }
    public void incDfsUsed(long value){
        used.addAndGet(value);
    }

    boolean running(){
        return running.get();
    }
    @VisibleForTesting
    public long getRefreshInterval() {
        return refreshInterval;
    }
    @VisibleForTesting
    public long getJitter() {
        return jitter;
    }
    protected void setUsed(long usedValue){
        this.used.set(usedValue);
    }

    @Override
    public void close() throws IOException {
        running.set(false);
        if (refreshUsed != null) {
            refreshUsed.interrupt();
        }
    }

    private static final class RefreshThread implements Runnable{
        final CachingGetSpaceUsed spaceUsed;
        private boolean runImmediately;
        RefreshThread(CachingGetSpaceUsed spaceUsed,boolean runImmediately){
            this.spaceUsed=spaceUsed;
            this.runImmediately=runImmediately;
        }

        @Override
        public void run() {
            while (spaceUsed.running()) {
                try {
                    long refreshInterval= spaceUsed.refreshInterval;
                    if (spaceUsed.jitter > 0) {
                        long jitter = spaceUsed.jitter;
                        refreshInterval= ThreadLocalRandom.current()
                                .nextLong(-jitter,jitter);
                    }
                    refreshInterval=Math.max(refreshInterval,1);
                    if (!runImmediately) {
                        Thread.sleep(refreshInterval);
                    }
                    runImmediately=false;
                    spaceUsed.refresh();
                }catch (InterruptedException e){
                    LOG.warn("Thread Interrupted waiting to refresh disk information: "+e.getMessage());
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
