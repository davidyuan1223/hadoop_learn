package org.apache.hadoop.metrics2;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.thirdparty.com.google.common.base.Objects;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import java.util.StringJoiner;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class AbstractMetric implements MetricsInfo{
    private final MetricsInfo info;
    protected AbstractMetric(MetricsInfo info){
        this.info= Preconditions.checkNotNull(info,"metric info");
    }

    @Override
    public String name() {
        return info.name();
    }

    @Override
    public String description() {
        return info.description();
    }

    public MetricsInfo info() {
        return info;
    }
    public abstract Number value();
    public abstract MetricType type();
    public abstract void visit(MetricsVisitor visitor);

    @Override
    public boolean equals(Object o) {
        if (o instanceof AbstractMetric) {
            final AbstractMetric other=(AbstractMetric) o;
            return Objects.equal(info,other.name())
                    &&Objects.equal(value(),other.value());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(info,value());
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", this.getClass().getSimpleName() + "{", "}")
                .add("info=" + info)
                .add("value=" + value())
                .toString();
    }
}
