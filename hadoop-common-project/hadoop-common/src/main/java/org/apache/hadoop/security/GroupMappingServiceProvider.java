package org.apache.hadoop.security;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface GroupMappingServiceProvider {
    String GROUP_MAPPING_CONFIG_PREFIX= CommonConfigurationKeysPublic.HADOOP_SECURITY_GROUP_MAPPING;
    List<String > getGroups(String user)throws IOException;
    void cacheGroupsRefresh()throws IOException;
    void cacheGroupsAdd()throws IOException;
    default Set<String > getGroupsSet(String user)throws IOException{
        return new LinkedHashSet<>(getGroups(user));
    }
}
