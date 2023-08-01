package org.apache.hadoop.http;

import org.apache.hadoop.conf.Configuration;

public abstract class FilterInitializer {
    public abstract void initFilter(FilterContainer container, Configuration conf);
}
