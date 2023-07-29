package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.thirdparty.com.google.common.collect.ComparisonChain;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class ElasticByteBufferPool  implements ByteBufferPool{
    protected static final class Key implements Comparable<Key>{
        private final int capacity;
        private final long insertionTime;
        Key(int capacity,long insertionTime){
            this.capacity=capacity;
            this.insertionTime=insertionTime;
        }

        @Override
        public int compareTo(Key o) {
            return ComparisonChain.start()
                    .compare(capacity,o.capacity)
                    .compare(insertionTime,o.insertionTime)
                    .result();
        }

        @Override
        public boolean equals(Object o) {
            if (o == null) {
                return false;
            }
            try {
                Key other=(Key) o;
                return (compareTo(other)==0);
            }catch (ClassCastException e){
                return false;
            }
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder()
                    .append(capacity)
                    .append(insertionTime)
                    .toHashCode();
        }
    }
    private final TreeMap<Key, ByteBuffer> buffers=new TreeMap<>();
    private final TreeMap<Key,ByteBuffer> directBuffers=new TreeMap<>();
    private final TreeMap<Key,ByteBuffer> getBufferTree(boolean direct){
        return direct?directBuffers:buffers;
    }

    @Override
    public synchronized ByteBuffer getBuffer(boolean direct, int length) {
        TreeMap<Key, ByteBuffer> tree = getBufferTree(direct);
        Map.Entry<Key, ByteBuffer> entry = tree.ceilingEntry(new Key(length, 0));
        if (entry == null) {
            return direct?ByteBuffer.allocateDirect(length):
                    ByteBuffer.allocate(length);
        }
        tree.remove(entry.getKey());
        entry.getValue().clear();
        return entry.getValue();
    }

    @Override
    public synchronized void putBuffer(ByteBuffer buffer) {
        buffer.clear();
        TreeMap<Key, ByteBuffer> tree = getBufferTree(buffer.isDirect());
        while (true) {
            Key key=new Key(buffer.capacity(),System.nanoTime());
            if (!tree.containsKey(key)) {
                tree.put(key,buffer);
                return;
            }
        }
    }
    @InterfaceAudience.Private
    @InterfaceStability.Unstable
    public int size(boolean direct){
        return getBufferTree(direct).size();
    }
}
