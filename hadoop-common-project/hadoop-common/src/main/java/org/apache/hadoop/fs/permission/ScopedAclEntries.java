package org.apache.hadoop.fs.permission;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.util.Collections;
import java.util.List;

@InterfaceAudience.LimitedPrivate({"HDFS","MapReduce"})
@InterfaceStability.Unstable
public final class ScopedAclEntries {
    private static final int PIVOT_NOT_FOUND=-1;
    private final List<AclEntry> accessEntries;
    private final List<AclEntry> defaultEntries;

    public ScopedAclEntries(List<AclEntry> aclEntries){
        int pivot=calculatePivotOnDefaultEntries(aclEntries);
        if (pivot != PIVOT_NOT_FOUND) {
            accessEntries=pivot!=0?aclEntries.subList(0,pivot): Collections.emptyList();
            defaultEntries=aclEntries.subList(pivot,aclEntries.size());
        }else {
            accessEntries=aclEntries;
            defaultEntries=Collections.emptyList();
        }
    }

    public List<AclEntry> getAccessEntries() {
        return accessEntries;
    }

    public List<AclEntry> getDefaultEntries() {
        return defaultEntries;
    }
    private static int calculatePivotOnDefaultEntries(List<AclEntry> aclBuilder){
        for (int i = 0; i < aclBuilder.size(); i++) {
            if (aclBuilder.get(i).getScope() == AclEntryScope.DEFAULT) {
                return i;
            }
        }
        return PIVOT_NOT_FOUND;
    }
}
