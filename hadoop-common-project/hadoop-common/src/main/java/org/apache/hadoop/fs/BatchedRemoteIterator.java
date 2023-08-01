package org.apache.hadoop.fs;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/30
 **/
public abstract class BatchedRemoteIterator<K,E> implements RemoteIterator<E> {
    public interface BatchedEntries<E>{
        E get(int i);
        int size();
        boolean hasMore();
    }
    public static class BatchedListEntries<E> implements BatchedEntries<E>{
        private final List<E> entries;
        private final boolean hasMore;
        public BatchedListEntries(List<E> entries,boolean hasMore){
            this.entries=entries;
            this.hasMore=hasMore;
        }
        public E get(int i){
            return entries.get(i);
        }

        @Override
        public int size() {
            return entries.size();
        }

        @Override
        public boolean hasMore() {
            return hasMore;
        }
    }
    private K prevKey;
    private BatchedEntries<E> entries;
    private int idx;

    public BatchedRemoteIterator(K prevKey) {
        this.prevKey = prevKey;
        this.entries=null;
        this.idx=-1;
    }
    public abstract BatchedEntries<E> makeRequest(K prevKey);
    private void makeRequest(){
        idx=0;
        entries=null;
        entries=makeRequest(prevKey);
        if (entries.size() == 0) {
            entries=null;
        }
    }
    private void makeRequestIfNeed(){
        if (idx == -1) {
            makeRequest();
        } else if ((entries != null) && (idx >= entries.size())) {
            if (!entries.hasMore()) {
                entries=null;
            }else {
                makeRequest();
            }
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        makeRequestIfNeed();
        return entries!=null;
    }
    public abstract K elementToPrevKey(E element);

    @Override
    public E next() throws IOException {
        makeRequestIfNeed();
        if (entries == null) {
            throw new NoSuchElementException();
        }
        E entry = entries.get(idx++);
        prevKey=elementToPrevKey(entry);
        return entry;
    }
}
