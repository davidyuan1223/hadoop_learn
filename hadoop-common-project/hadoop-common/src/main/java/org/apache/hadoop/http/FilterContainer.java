package org.apache.hadoop.http;

import java.util.Map;

public interface FilterContainer {
    void addFilter(String name, String classname, Map<String ,String > parameters);
    void addGlobalFilter(String name,String classname,Map<String ,String > parameters);
}
