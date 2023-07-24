package org.apache.hadoop.conf;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/22
 **/
public class ReconfigurationUtil {
    public static class PropertyChange{
        public String prop;
        public String oldVal;
        public String newVal;
        public PropertyChange(String prop,String oldVal,String newVal){
            this.prop=prop;
            this.oldVal=oldVal;
            this.newVal=newVal;
        }
    }
    public static Collection<PropertyChange> getChangedProperties(Configuration newConf,Configuration oldConf){
        Map<String ,PropertyChange> changes=new HashMap<>();
        for (Map.Entry<String, String> oldEntry : oldConf) {
            String prop = oldEntry.getKey();
            String oldVal = oldEntry.getValue();
            String newVal = newConf.getRaw(prop);
            if (newVal == null || !newVal.equals(oldVal)) {
                changes.put(prop,new PropertyChange(prop,oldVal,newVal));
            }
        }
        for (Map.Entry<String, String> newEntry : newConf) {
            String prop = newEntry.getKey();
            String newVal = newEntry.getValue();
            if (oldConf.get(prop) == null) {
                changes.put(prop,new PropertyChange(prop,null,newVal));
            }
        }
        return changes.values();
    }
    public Collection<PropertyChange> parseChangedProperties(Configuration newConf,Configuration oldConf){
        return getChangedProperties(newConf,oldConf);
    }
}
