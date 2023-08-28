package org.apache.hadoop.metrics2.util;

import com.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.thirdparty.com.google.common.collect.ComparisonChain;

/**
 * Specifies a quantile (with error bounds) to be watched by a SampleQuantile object.
 */
@InterfaceAudience.Private
public class Quantile implements Comparable<Quantile>{
    public final double quantile;
    public final double error;

    public Quantile(double quantile,double error) {
        this.quantile = quantile;
        this.error = error;
    }

    @Override
    public boolean equals(Object obj) {
        if (this==obj) {
            return true;
        }
        if (!(obj instanceof Quantile)) {
            return false;
        }
        Quantile other = (Quantile) obj;
        long qbits=Double.doubleToLongBits(quantile);
        long ebits=Double.doubleToLongBits(error);

        return Double.doubleToLongBits(other.quantile) == qbits && ebits==Double.doubleToLongBits(error);
    }

    @Override
    public int hashCode() {
        return (int) (Double.doubleToLongBits(quantile) ^ Double.doubleToLongBits(error));
    }

    @Override
    public int compareTo(Quantile o) {
        return ComparisonChain.start()
                .compare(quantile,o.quantile)
                .compare(error,o.error)
                .result();
    }

    @Override
    public String toString() {
        return String.format("%.2f %%ile +/- %.2f%%",quantile*100,error*100);
    }
}
