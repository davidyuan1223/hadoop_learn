package org.apache.hadoop.metrics2;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.thirdparty.com.google.common.base.Objects;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import java.util.StringJoiner;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MetricsTag implements MetricsInfo{
    private final MetricsInfo info;
    private final String value;

    public MetricsTag(MetricsInfo info,String value){
        this.info= Preconditions.checkNotNull(info,"tag info");
        this.value=value;
    }

    @Override
    public String name() {
        return info.name();
    }

    @Override
    public String description() {
        return info.description();
    }

    public MetricsInfo info(){
        return info;
    }
    public String value(){
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof MetricsTag) {
            final MetricsTag other=(MetricsTag) o;
            return org.apache.hadoop.thirdparty.com.google.common.base.Objects.equal(info,other.info())
                    && org.apache.hadoop.thirdparty.com.google.common.base.Objects.equal(value,other.value());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(info, value);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", this.getClass().getSimpleName() + "{", "}")
                .add("info=" + info)
                .add("value=" + value())
                .toString();
    }
}
