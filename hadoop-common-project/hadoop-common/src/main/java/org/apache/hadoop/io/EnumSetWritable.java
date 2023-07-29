package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.AbstractCollection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Objects;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class EnumSetWritable <E extends Enum<E>> extends AbstractCollection<E> implements Writable, Configurable {
    private EnumSet<E> value;
    private transient Class<E> elementType;
    private transient Configuration conf;
    EnumSetWritable(){}

    @Override
    public Iterator<E> iterator() {
        return value.iterator();
    }

    @Override
    public int size() {
        return value.size();
    }

    @Override
    public boolean add(E e) {
        if (value == null) {
            value=EnumSet.of(e);
            set(value,null);
        }
        return value.add(e);
    }
    public EnumSetWritable(EnumSet<E> value,Class<E> elementType){
        set(value,elementType);
    }
    public EnumSetWritable(EnumSet<E> value){
        this(value,null);
    }
    public void set(EnumSet<E> value,Class<E> elementType) {
        if ((value == null || value.size() == 0)
                && (this.elementType == null && elementType == null)) {
            throw new IllegalArgumentException("The EnumSet argument is null,or is an empty set but with no elementType provided.");
        }
        this.value = value;
        if (value != null && value.size() > 0) {
            this.elementType = value.iterator().next().getDeclaringClass();
        } else if (elementType != null) {
            this.elementType = elementType;
        }
    }
    public EnumSet<E> get(){
        return value;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void readFields(DataInput in) throws IOException {
        int length = in.readInt();
        if (length==-1) {
            this.value=null;
        } else if (length == 0) {
            this.elementType=(Class<E>) ObjectWritable.loadClass(conf,
                    WritableUtils.readString(in));
        }else {
            E first=(E) ObjectWritable.readObject(in,conf);
            this.value=EnumSet.of(first);
            for (int i = 0; i < length; i++) {
                this.value.add((E)ObjectWritable.readObject(in,conf));
            }
        }
    }

    @Override
    public void writer(DataOutput out) throws IOException {
        if (this.value == null) {
            out.write(-1);
            WritableUtils.writeString(out,this.elementType.getName());
        }else {
            Object[] array = this.value.toArray();
            int length = array.length;
            out.writeInt(length);
            if (length==0) {
                if (this.elementType == null) {
                    throw new UnsupportedOperationException("Unable to serialize empty EnumSet with no element type provided.");
                }
                WritableUtils.writeString(out,this.elementType.getName());
            }
            for (Object o : array) {
                ObjectWritable.writeObject(out, o, o.getClass(), conf);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            throw new IllegalArgumentException("null argument passed in equal().");
        }
        if (!(o instanceof EnumSetWritable)) {
            return false;
        }
        EnumSetWritable<?> other=(EnumSetWritable<?>) o;
        if (this == o || this.value == other.value) {
            return true;
        }
        if (this.value == null) {
            return false;
        }
        return this.value.equals(other.value);
    }
    public Class<E> getElementType(){
        return elementType;
    }

    @Override
    public int hashCode() {
        if (value == null) {
            return 0;
        }
        return value.hashCode();
    }

    @Override
    public String toString() {
        if (value == null) {
            return "(null)";
        }
        return value.toString();
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }
    static {
        WritableFactories.setFactory(EnumSetWritable.class, new WritableFactory() {
            @Override
            public Writable newInstance() {
                return new EnumSetWritable();
            }
        });
    }
}
