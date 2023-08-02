package org.apache.hadoop.fs.statistics;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.io.Serializable;
import java.util.Objects;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MeanStatistic implements Serializable,Cloneable {
    private static final long serialVersionUID=567888327998615425L;
    private long samples;
    private long sum;
    public MeanStatistic(final long samples,final long sum){
        if (samples > 0) {
            this.sum=sum;
            this.samples=samples;
        }
    }
    public MeanStatistic(MeanStatistic o){
        synchronized (o){
            set(o);
        }
    }
    public MeanStatistic(){}
    public synchronized long getSum(){return sum;}
    public synchronized long getSamples(){return samples;}
    @JsonIgnore
    public synchronized boolean isEmpty(){return samples==0;}
    public void clear(){
        setSamplesAndSum(0,0);
    }
    public synchronized void setSamplesAndSum(long samples,long sum){
        setSamples(samples);
        setSum(sum);
    }
    public void set(final MeanStatistic o){
        setSamplesAndSum(o.getSamples(),o.getSum());
    }
    public synchronized void setSum(long sum){
        this.sum=sum;
    }
    public synchronized void setSamples(long samples){
        if (samples<0){
            this.samples=0;
        }else {
            this.samples=samples;
        }
    }
    public synchronized double mean(){
        return samples>0?((double) sum)/samples:0.0d;
    }
    public synchronized MeanStatistic add(final MeanStatistic o){
        if (o.isEmpty()) {
            return this;
        }
        long oSum;
        long oSamples;
        synchronized (o){
            oSamples=o.samples;
            oSum=o.sum;
        }
        if (isEmpty()) {
            samples=oSamples;
            sum=oSum;
            return this;
        }
        samples+=oSamples;
        sum+=oSum;
        return this;
    }

    public synchronized void addSamples(long value){
        samples++;
        sum+=value;
    }

    @Override
    public synchronized boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MeanStatistic that = (MeanStatistic) o;
        if (isEmpty()) {
            return that.isEmpty();
        }
        return samples == that.samples && sum == that.sum;
    }

    @Override
    public int hashCode() {
        return Objects.hash(samples, sum);
    }

    public MeanStatistic copy(){
        return new MeanStatistic(this);
    }

    @Override
    public String toString() {
        return String.format("(samples=%d, sum=%d, mean=%.4f)",samples,sum,mean());
    }
}
