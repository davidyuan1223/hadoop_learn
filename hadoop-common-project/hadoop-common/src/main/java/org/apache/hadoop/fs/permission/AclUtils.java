package org.apache.hadoop.fs.permission;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@InterfaceAudience.LimitedPrivate({"HDFS","MapReduce"})
@InterfaceStability.Unstable
public final class AclUtils {
    public static List<AclEntry> getAclFromPermAndEntries(FsPermission perm,List<AclEntry> entries){
        ArrayList<AclEntry> acl = Lists.newArrayListWithCapacity(entries.size() + 3);
        acl.add(new AclEntry.Builder()
        .setScope(AclEntryScope.ACCESS)
        .setType(AclEntryType.USER)
        .setPermission(perm.getUserAction())
        .build());
        boolean hasAccessAcl=false;
        Iterator<AclEntry> entryIter = entries.iterator();
        AclEntry curEntry=null;
        while (entryIter.hasNext()) {
            curEntry=entryIter.next();
            if (curEntry.getScope() == AclEntryScope.DEFAULT) {
                break;
            }
            hasAccessAcl=true;
            acl.add(curEntry);
        }
        acl.add(new AclEntry.Builder()
        .setScope(AclEntryScope.ACCESS)
        .setType(hasAccessAcl?AclEntryType.MASK:AclEntryType.GROUP)
        .setPermission(perm.getGroupAction())
        .build());
        acl.add(new AclEntry.Builder()
                .setScope(AclEntryScope.ACCESS)
                .setType(AclEntryType.OTHER)
                .setPermission(perm.getOtherAction())
                .build());
        if (curEntry != null && curEntry.getScope() == AclEntryScope.DEFAULT) {
            acl.add(curEntry);
            while (entryIter.hasNext()) {
                acl.add(entryIter.next());
            }
        }
        return acl;
    }
    public static List<AclEntry> getMinimalAcl(FsPermission perm){
        return Lists.newArrayList(
                new AclEntry.Builder()
                .setScope(AclEntryScope.ACCESS)
                .setType(AclEntryType.USER)
                .setPermission(perm.getUserAction())
                .build(),
                new AclEntry.Builder()
                        .setScope(AclEntryScope.ACCESS)
                        .setType(AclEntryType.GROUP)
                        .setPermission(perm.getGroupAction())
                        .build(),
                new AclEntry.Builder()
                        .setScope(AclEntryScope.ACCESS)
                        .setType(AclEntryType.OTHER)
                        .setPermission(perm.getOtherAction())
                        .build()
        );
    }
    public static boolean isMinimalAcl(List<AclEntry> entries){
        return entries.size()==3;
    }
    private AclUtils(){}
}
