package org.apache.hadoop.util;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class IdentityHashStore<K,V> {
    private Object buffer[];
    private int numInserted=0;
    private int capacity;
    private static final int DEFAULT_MAX_CAPACITY=2;

    public IdentityHashStore(int capacity){
        Preconditions.checkArgument(capacity>=0);
        if (capacity==0) {
            this.capacity=0;
            this.buffer=null;
        }else {
            realloc((int)Math.pow(2,Math.ceil(Math.log(capacity)/Math.log(2))));
        }
    }

    private void realloc(int newCapacity){
        Preconditions.checkArgument(newCapacity>0);
        Object[] prevBuffer=buffer;
        this.capacity=newCapacity;
        this.buffer=new Object[4*newCapacity];
        if (prevBuffer != null) {
            for (int i = 0; i < prevBuffer.length; i++) {
                if (prevBuffer[i] != null) {
                    putInternal(prevBuffer[i],prevBuffer[i+1]);
                }
            }
        }
    }

    private void putInternal(Object k,Object v){
        final int hash=System.identityHashCode(k);
        final int numEntries=buffer.length>>1;
        int index=hash&(numEntries-1);
        while (true) {
            if (buffer[2 * index] == null) {
                buffer[2*index]=k;
                buffer[1+(2*index)]=v;
                numInserted++;
                return;
            }
            index=(index+1)%numEntries;
        }
    }

    public void put(K k,V v){
        Preconditions.checkNotNull(k);
        if (buffer == null) {
            realloc(DEFAULT_MAX_CAPACITY);
        } else if (numInserted + 1 > capacity) {
            realloc(capacity*2);
        }
        putInternal(k,v);
    }

    private int getElementIndex(K k){
        if (buffer == null) {
            return -1;
        }
        final int numEntries=buffer.length>>1;
        final int hash=System.identityHashCode(k);
        int index=hash&(numEntries-1);
        int firstIndex=index;
        do{
            if (buffer[2 * index] == k) {
                return index;
            }
            index=(index+1)%numEntries;
        }while (index!=firstIndex);
        return -1;
    }

    public V get(K k){
        int index = getElementIndex(k);
        if (index < 0) {
            return null;
        }
        return (V)buffer[1 + (2 * index)];
    }

    public V remove(K k){
        int index = getElementIndex(k);
        if (index < 0) {
            return null;
        }
        V val=(V)buffer[1+(2*index)];
        buffer[2*index]=null;
        buffer[1+(2*index)]=null;
        numInserted--;
        return val;
    }

    public boolean isEmpty(){
        return numInserted==0;
    }
    public int numElements(){
        return numInserted;
    }
    public int capacity(){
        return capacity;
    }
    public interface Visitor<K,V> {
        void accept(K k,V v);
    }
    public void visitAll(Visitor<K,V> visitor){
        int length=buffer==null?0:buffer.length;
        for (int i = 0; i < length; i++) {
            if (buffer[i] != null) {
                visitor.accept((K)buffer[i],(V)buffer[i+1]);
            }
        }
    }
}
