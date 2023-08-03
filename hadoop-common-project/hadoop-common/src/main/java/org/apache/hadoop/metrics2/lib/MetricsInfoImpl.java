package org.apache.hadoop.metrics2.lib;

import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.thirdparty.com.google.common.base.Objects;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import java.util.StringJoiner;

public class MetricsInfoImpl implements MetricsInfo {
    private final String name,description;
    public MetricsInfoImpl(String name, String desc) {
        this.name= Preconditions.checkNotNull(name,"name");
        this.description=Preconditions.checkNotNull(desc,"description");
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String description() {
        return description;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof MetricsInfo) {
            MetricsInfo other=(MetricsInfo) o;
            return Objects.equal(name,other.name())&&
                    Objects.equal(description,other.description());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name,description);
    }
    @Override public String toString() {
        return new StringJoiner(", ", this.getClass().getSimpleName() + "{", "}")
                .add("name=" + name)
                .add("description=" + description)
                .toString();
    }
}
