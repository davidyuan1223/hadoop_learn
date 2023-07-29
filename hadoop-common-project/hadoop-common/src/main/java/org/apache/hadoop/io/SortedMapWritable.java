package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.util.*;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class SortedMapWritable<K extends WritableComparable<? extends K>> extends AbstractMapWritable implements SortedMap<K,Writable> {
    private SortedMap<K,Writable> instance;
    public SortedMapWritable(){
        super();
        this.instance=new TreeMap<>();
    }
    public SortedMapWritable(SortedMapWritable<K> other){
        this();
        copy(other);
    }

    @Override
    public Comparator<? super K> comparator() {
        return null;
    }

    @Override
    public K firstKey() {
        return instance.firstKey();
    }

    @Override
    public SortedMap<K, Writable> headMap(K toKey) {
        return instance.headMap(toKey);
    }

    @Override
    public K lastKey() {
        return instance.lastKey();
    }

    @Override
    public SortedMap<K, Writable> subMap(K fromKey, K toKey) {
        return instance.subMap(fromKey,toKey);
    }

    @Override
    public SortedMap<K, Writable> tailMap(K fromKey) {
        return instance.tailMap(fromKey);
    }

    @Override
    public void clear() {
        instance.clear();
    }

    @Override
    public boolean containsKey(Object key) {
        return instance.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return instance.containsValue(value);
    }

    @Override
    public Set<Entry<K, Writable>> entrySet() {
        return instance.entrySet();
    }

    @Override
    public Writable get(Object key) {
        return instance.get(key);
    }

    @Override
    public boolean isEmpty() {
        return instance.isEmpty();
    }

    @Override
    public Set<K> keySet() {
        return instance.keySet();
    }

    @Override
    public Writable put(K key, Writable value) {
        addToMap(key.getClass());
        addToMap(value.getClass());
        return instance.put(key,value);
    }

    @Override
    public void putAll(Map<? extends K, ? extends Writable> m) {
        for (Entry<? extends K, ? extends Writable> entry : m.entrySet()) {
            put(entry.getKey(),entry.getValue());
        }
    }

    @Override
    public boolean remove(Object key, Object value) {
        return instance.remove(key,value);
    }

    @Override
    public Writable remove(Object key) {
        return instance.remove(key);
    }

    @Override
    public int size() {
        return instance.size();
    }

    @Override
    public Collection<Writable> values() {
        return instance.values();
    }


}
