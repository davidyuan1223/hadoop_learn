package org.apache.hadoop.conf;

import java.util.Collection;

public interface Reconfigurable extends Configurable{
    void reconfigureProperty(String property,String newVal);

    boolean isPropertyReconfigurable(String property);

    Collection<String > getReconfigurableProperties();
}
