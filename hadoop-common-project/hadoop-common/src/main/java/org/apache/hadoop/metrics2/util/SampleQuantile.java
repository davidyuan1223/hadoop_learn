package org.apache.hadoop.metrics2.util;

import com.apache.hadoop.classification.InterfaceAudience;

import java.util.LinkedList;

/**
 * Implementation of the Cormode,Korn,Muthukrishnan,and Srivastava algorithms
 * for streaming caculation of targeted high-percentile epsilon-approximate
 * quantiles.
 *
 * This is a generalization of the earlier work by Greenwald and Khanna (GK),
 * which essentially allows different error bounds on the targeted quantiles,
 * which allows for fa more efficient calculation of high-percentile.
 *
 * @see <a href="http://en.wikipedia.org/wiki/Quantile"
 */
@InterfaceAudience.Private
public class SampleQuantile implements QuantileEstimator {
    private long count=0;
    private LinkedList<SampleItem> samples;
    private long[] buffer=new long[500];
    private int bufferCount=0;
    private final Quantile[] quantiles;
    public SampleQuantile(Quantile[] quantiles){
        this.quantiles=quantiles;
        samples=new LinkedList<SampleItem>();
    }
    private double allowableError(int rank){
        int size = samples.size();
        double minError=size+1;
        for (Quantile quantile : quantiles) {
            double error;
            if (rank <= quantile.quantile * size) {
                error=(2.0* quantile.error*(size-rank))/(1.0- quantile.quantile);
            }else {
                error=(2.0* quantile.error*rank)/ quantile.quantile;
            }
            if (error < minError) {
                minError=error;
            }
        }
        return minError;
    }
    synchronized public void insert(long v){
        buffer[bufferCount]=v;
        bufferCount++;
        count++;
        if (bufferCount == buffer.length) {
            insertBatch();
            compress();
        }
    }
    private void insertBatch(){
        int start=0;
        if (samples.size() == 0) {
            SampleItem newItem = new SampleItem(buffer[0], 1, 0);
        }
    }















    private static class SampleItem{
        public final long value;
        public int g;
        public final int delta;
        public SampleItem(int value,int lowerDelta,int delta){
            this.value=value;
            this.g=lowerDelta;
            this.delta=delta;
        }

        @Override
        public String toString() {
            return String.format("%d, %d, %d", value, g, delta);
        }
    }
}
