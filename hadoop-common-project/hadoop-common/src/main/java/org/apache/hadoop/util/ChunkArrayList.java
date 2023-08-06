package org.apache.hadoop.util;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.Iterables;

import java.util.AbstractList;
import java.util.Iterator;
import java.util.List;

@InterfaceAudience.Private
public class ChunkArrayList<T> extends AbstractList<T> {
    private final List<List<T>> chunks = Lists.newArrayList();
    private List<T> lastChunk = null;
    private int lastChunkCapacity;
    private final int initialChunkCapacity;
    private final int maxChunkSize;
    private int size;
    private static final int DEFAULT_INITIAL_CHUNK_CAPACITY = 6;
    private static final int DEFAULT_MAX_CHUNK_SIZE = 8 * 1024;

    public ChunkArrayList() {
        this(DEFAULT_INITIAL_CHUNK_CAPACITY, DEFAULT_MAX_CHUNK_SIZE);
    }

    public ChunkArrayList(int initialChunkCapacity, int maxChunkSize) {
        Preconditions.checkArgument(maxChunkSize >= initialChunkCapacity);
        this.initialChunkCapacity = initialChunkCapacity;
        this.maxChunkSize = maxChunkSize;
    }

    @Override
    public Iterator<T> iterator() {
        final Iterator<T> it = Iterables.concat(chunks).iterator();
        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public T next() {
                return it.next();
            }

            @Override
            public void remove() {
                it.remove();
                size--;
            }
        };
    }

    @Override
    public boolean add(T t) {
        if (size == Integer.MAX_VALUE) {
            throw new RuntimeException("Can't add an additional element to the list; list already has INT_MAX elements");
        }
        if (lastChunk == null) {
            addChunk(initialChunkCapacity);
        } else if (lastChunk.size() >= lastChunkCapacity) {
            int newCapacity = lastChunkCapacity + (lastChunkCapacity >> 1);
            addChunk(Math.min(newCapacity, maxChunkSize));
        }
        size++;
        return lastChunk.add(t);
    }

    @Override
    public void clear() {
        chunks.clear();
        lastChunk=null;
        lastChunkCapacity=0;
        size=0;
    }

    private void addChunk(int capacity){
        lastChunk=Lists.newArrayListWithCapacity(capacity);
        chunks.add(lastChunk);
        lastChunkCapacity=capacity;
    }

    @Override
    public boolean isEmpty() {
        return size==0;
    }

    @Override
    public int size() {
        return size;
    }
    @VisibleForTesting
    int getNumChunks(){
        return chunks.size();
    }
    @VisibleForTesting
    int getMaxChunkSize(){
        int size=0;
        for (List<T> chunk : chunks) {
            size=Math.max(size,chunk.size());
        }
        return size;
    }

    @Override
    public T get(int index) {
        if (index < 0) {
            throw new IndexOutOfBoundsException();
        }
        int base=0;
        Iterator<List<T>> it = chunks.iterator();
        while (it.hasNext()) {
            List<T> list = it.next();
            int size = list.size();
            if (index < base + size) {
                return list.get(index-base);
            }
            base+=size;
        }
        throw new IndexOutOfBoundsException();
    }
}
