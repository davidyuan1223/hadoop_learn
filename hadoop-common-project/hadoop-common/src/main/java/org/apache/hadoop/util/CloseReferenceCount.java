package org.apache.hadoop.util;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.atomic.AtomicInteger;

public class CloseReferenceCount {
    private static final int STATUS_CLOSED_MASK=1<<30;
    private final AtomicInteger status=new AtomicInteger(0);
    public CloseReferenceCount(){}
    public void reference()throws ClosedChannelException{
        int curBit = status.incrementAndGet();
        if ((curBit & STATUS_CLOSED_MASK)!=0) {
            status.decrementAndGet();
            throw new ClosedChannelException();
        }
    }
    public boolean unreference(){
        int newVal = status.decrementAndGet();
        Preconditions.checkState(newVal!=0xffffffff,
                "called unreference when the reference count was already at 0");
        return newVal==STATUS_CLOSED_MASK;
    }
    public void unreferenceCheckClosed()throws ClosedChannelException{
        int newVal = status.decrementAndGet();
        if ((newVal & STATUS_CLOSED_MASK) != 0) {
            throw new AsynchronousCloseException();
        }
    }
    public boolean isOpen(){
        return ((status.get()&STATUS_CLOSED_MASK)==0);
    }
    public int setClosed()throws ClosedChannelException{
        while (true) {
            int curBits = status.get();
            if ((curBits & STATUS_CLOSED_MASK) != 0) {
                throw new ClosedChannelException();
            }
            if (status.compareAndSet(curBits, curBits | STATUS_CLOSED_MASK)) {
                return curBits&(~STATUS_CLOSED_MASK);
            }
        }
    }
    public int getReferenceCount(){
        return status.get()&(~STATUS_CLOSED_MASK);
    }
}
