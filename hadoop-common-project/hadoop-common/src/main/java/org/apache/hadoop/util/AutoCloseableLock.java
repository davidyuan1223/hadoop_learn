package org.apache.hadoop.util;

import com.apache.hadoop.classification.VisibleForTesting;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/23
 **/
public class AutoCloseableLock implements AutoCloseable {
    private final Lock lock;
    public AutoCloseableLock(){
        this(new ReentrantLock());
    }
    public AutoCloseableLock(Lock lock){
        this.lock=lock;
    }
    public AutoCloseableLock acquire(){
        lock.lock();
        return this;
    }
    public void release(){
        lock.unlock();
    }

    @Override
    public void close() throws Exception {
        release();
    }
    public boolean tryLock(){
        return lock.tryLock();
    }
    @VisibleForTesting
    boolean isLocked(){
        if (lock instanceof ReentrantLock) {
            return ((ReentrantLock) lock).isLocked();
        }
        throw new UnsupportedOperationException();
    }
    public Condition newCondition(){
        return lock.newCondition();
    }
}
