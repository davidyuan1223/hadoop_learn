package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/23
 **/
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class BinaryComparable implements Comparable<BinaryComparable> {
    public abstract int getLength();
    public abstract byte[] getBytes();

    @Override
    public int compareTo(BinaryComparable o) {
        if (this==o){
            return 0;
        }
        return WritableComparator.compareBytes(getBytes(),0,getLength(),
                o.getBytes(),0,o.getLength());
    }
    public int compareTo(byte[] other,int off,int len){
        return WritableComparator.compareBytes(getBytes(),0,getLength(),
                other,off,len);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof BinaryComparable)) {
            return false;
        }
        BinaryComparable other=(BinaryComparable)obj;
        if (this.getLength() != other.getLength()) {
            return false;
        }
        return this.compareTo(other)==0;
    }

    @Override
    public int hashCode() {
        return WritableComparator.hashBytes(getBytes(),getLength());
    }
}
