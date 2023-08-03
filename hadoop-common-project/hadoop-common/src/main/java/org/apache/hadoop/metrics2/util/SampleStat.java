package org.apache.hadoop.metrics2.util;

import com.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
public class SampleStat {
    private final MinMax minmax=new MinMax();
    private long numSamples=0;
    private double mean,s;
    public SampleStat(){
        mean=0.0;
        s=0.0;
    }
    public void reset(){
        numSamples=0;
        mean=0.0;
        s=0.0;
        minmax.reset();
    }
    void reset(long numSamples1, double mean1, double s1, MinMax minmax1) {
        numSamples = numSamples1;
        mean = mean1;
        s = s1;
        minmax.reset(minmax1);
    }
    public void copyTo(SampleStat other){
        other.reset(numSamples,mean,s,minmax);
    }
    public SampleStat add(double x){
        minmax.add(x);
        return add(1,x);
    }
    public SampleStat add(long nSamples,double xTotal){
        numSamples+=nSamples;
        double x=xTotal/nSamples;
        double meanOld=mean;
        mean+=((double) nSamples/numSamples)*(x-meanOld);
        s+=nSamples*(x-meanOld)*(x-mean);
        return this;
    }

    public long numSamples(){return numSamples;}
    public double total(){return mean*numSamples;}
    public double mean(){return numSamples>0?mean:0.0;}
    public double variance(){return numSamples>1?s/(numSamples-1):0.0;}
    public double stddev(){return Math.sqrt(variance());}
    public double min() {
        return minmax.min();
    }

    /**
     * @return  the maximum value of the samples
     */
    public double max() {
        return minmax.max();
    }

    @Override
    public String toString() {
        try {
            return "Samples = " + numSamples() +
                    "  Min = " + min() +
                    "  Mean = " + mean() +
                    "  Std Dev = " + stddev() +
                    "  Max = " + max();
        } catch (Throwable t) {
            return super.toString();
        }
    }


    @SuppressWarnings("PublicInnerClass")
    public static class MinMax{
        static final double DEFAULT_MIN_VALUE=Float.MAX_VALUE;
        static final double DEFAULT_MAX_VALUE=Float.MIN_VALUE;
        private double min=DEFAULT_MIN_VALUE;
        private double max=DEFAULT_MAX_VALUE;
        public void add(double value){
            if (value>max) max=value;
            if (value<min) min=value;
        }
        public double min(){return min;}
        public double max(){return max;}
        public void reset(){
            min=DEFAULT_MIN_VALUE;
            max=DEFAULT_MAX_VALUE;
        }
        public void reset(MinMax other){
            min=other.min();
            max=other.max();
        }
    }
}
