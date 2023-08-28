package org.apache.hadoop.crypto.key.kms;

import com.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.cache.LoadingCache;

import javax.naming.Name;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;

@InterfaceAudience.Private
public class ValueQueue <E>{
    public interface QueueRefiller<E>{
        void fillQueueForKey(String keyName, Queue<E> keyQueue, int numValues)throws IOException;
    }
    private static class UniqueKeyBlockingQueue extends LinkedBlockingQueue<Runnable>{
        private static final long serialVersionUID=-123456L;
        private HashMap<String ,Runnable> keysInProgress=new HashMap<>();

        @Override
        public void put(Runnable runnable) throws InterruptedException {
            if (!keysInProgress.containsKey((NamedRunnable)runnable).name){
                keysInProgress.put(((NamedRunnable)runnable).name,runnable);
                super.put(runnable);
            }
        }

        @Override
        public Runnable take() throws InterruptedException {
            Runnable k = super.take();
            if (k != null) {
                keysInProgress.remove(((NamedRunnable)k).name);
            }
            return k;
        }

        @Override
        public Runnable poll(long timeout, TimeUnit unit) throws InterruptedException {
            Runnable k = super.poll(timeout, unit);
            if (k != null) {
                keysInProgress.remove(((NamedRunnable)k).name);
            }
            return k;
        }
        public Runnable deleteByname(String name){
            NamedRunnable e=(NamedRunnable) keysInProgress.remove(name);
            if (e != null) {
                e.cancel();
                super.remove(e);
            }
            return e;
        }
    }
    private abstract static class NamedRunnable implements Runnable{
        final String name;
        private AtomicBoolean canceled=new AtomicBoolean(false);
        private NamedRunnable(String keyname){
            this.name=keyname;
        }
        public void cancel(){
            canceled.set(true);
        }
        public boolean isCanceled(){
            return canceled.get();
        }
    }
    public enum SyncGenerationPolicy{
        ATLEAST_ONE,
        LOW_WATERMARK,
        ALL
    }
    private static final String REFILL_THREAD=ValueQueue.class.getName()+"_thread";
    private static final int LOCK_ARRAY_SIZE=16;
    private static final int MASK=LOCK_ARRAY_SIZE==Integer.MAX_VALUE?
            LOCK_ARRAY_SIZE:
            LOCK_ARRAY_SIZE-1;

    private final LoadingCache<String , LinkedBlockingQueue<E>> keyQueues;
    private final List<ReadWriteLock> lockArray=new ArrayList<>(LOCK_ARRAY_SIZE);
    private final ThreadPoolExecutor executor;
    private final UniqueKeyBlockingQueue queue=new UniqueKeyBlockingQueue();
    private final QueueRefiller<E> refiller;
    private final SyncGenerationPolicy policy;
    private final int numValues;
    private final float lowWatermark;
    private volatile boolean executorThreadsStarted=false;
    private void readLock(String keyName){
        getLock(keyName).readLock().lock();
    }
    private void readUnLock(String keyName){
        getLock(keyName).readLock().unlock();
    }
    private void writeLock(String keyName){
        getLock(keyName).writeLock().lock();
    }
    private void writeUnLock(String keyName){
        getLock(keyName).writeLock().unlock();
    }
    private ReadWriteLock getLock(String keyName){
        return lockArray.get(indexFor(keyName));
    }
    private static int indexFor(String keyName){
        return keyName.hashCode() & MASK;
    }
    public ValueQueue(final int numValues,final float lowWatermark,
                      long expiry,int numFillerThreads,SyncGenerationPolicy policy,
                      final QueueRefiller<E> refiller){
        Preconditions.checkArgument(numValues>0,"\"numValues\" must be >0");
        
    }
}
