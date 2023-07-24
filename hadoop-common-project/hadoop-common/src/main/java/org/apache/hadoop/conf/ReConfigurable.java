package org.apache.hadoop.conf;

import java.util.Collection;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/22
 **/
public interface ReConfigurable extends Configurable {
    void reconfigureProperty(String property,String newVal) throws ReconfigurationException;
    boolean isPropertyReconfigurable(String property);
    Collection<String > getReconfigurableProperties();
}
