package org.apache.hadoop.security;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

@InterfaceAudience.LimitedPrivate({"HDFS","MapReduce"})
@InterfaceStability.Evolving
public class CompositeGroupsMapping implements GroupMappingServiceProvider, Configurable {
    private static final String MAPPING_PROVIDERS_CONFIG_KEY=GROUP_MAPPING_CONFIG_PREFIX+".providers";
    private static final String MAPPING_PROVIDERS_COMBINED_CONFIG_KEY=MAPPING_PROVIDERS_CONFIG_KEY+".combined";
    private static final String MAPPING_PROVIDERS_CONFIG_PREFIX=GROUP_MAPPING_CONFIG_PREFIX+".provider";
    private static final Logger LOG= LoggerFactory.getLogger(CompositeGroupsMapping.class);
    private List<GroupMappingServiceProvider> providerList=new ArrayList<>();
    private Configuration conf;
    private boolean combined;

    @Override
    public synchronized List<String> getGroups(String user) throws IOException {
        Set<String > groupSet=new TreeSet<>();
        for (GroupMappingServiceProvider provider : providerList) {
            List<String > groups= Collections.emptyList();
            try {
                groups=provider.getGroups(user);
            }catch (Exception e){
                LOG.warn("Unable to get groups for user {} via {} because {}",
                        user,provider.getClass().getSimpleName(),e.toString());
                LOG.debug("Stacktrace: ",e);
            }
            if (!groups.isEmpty()) {
                groupSet.addAll(groups);
                if (!combined) break;
            }
        }
        return new ArrayList<>(groupSet);
    }

    @Override
    public void cacheGroupsRefresh() throws IOException {

    }

    @Override
    public void cacheGroupsAdd() throws IOException {

    }

    @Override
    public synchronized Set<String> getGroupsSet(String user) throws IOException {
        Set<String > groupSet=new HashSet<>();
        Set<String > groups=null;
        for (GroupMappingServiceProvider provider : providerList) {
            try {
                groups=provider.getGroupsSet(user);
            }catch (Exception e){
                LOG.warn("Unable to get groups for user {} via {} because {}",
                        user,provider.getClass().getSimpleName(),e.toString());
                LOG.debug("Stacktrace: ",e);
            }
            if (groups != null && !groups.isEmpty()) {
                groupSet.addAll(groups);
                if (!combined) {
                    break;
                }
            }
        }
        return groupSet;
    }

    @Override
    public synchronized Configuration getConf() {
        return conf;
    }

    @Override
    public synchronized void setConf(Configuration conf) {
        this.conf=conf;
        this.combined=conf.getBoolean(MAPPING_PROVIDERS_COMBINED_CONFIG_KEY,true);
        loadMappingProviders();
    }
    private void loadMappingProviders(){
        String[] providerNames = conf.getStrings(MAPPING_PROVIDERS_CONFIG_KEY, new String[]{});
        String providerKey;
        for (String name : providerNames) {
            providerKey=MAPPING_PROVIDERS_CONFIG_PREFIX+"."+name;
            Class<?> providerClass = conf.getClass(providerKey, null);
            if (providerClass == null) {
                LOG.error("The mapping provider, "+name+" does not have a valid class");
            }else {
                addMappingProvider(name,providerClass);
            }
        }
    }
    private void addMappingProvider(String providerName,Class<?> providerClass){
        Configuration newConf=prepareConf(providerName);
        GroupMappingServiceProvider provider=
                (GroupMappingServiceProvider) ReflectionUtils.newInstance(providerClass,newConf);
        providerList.add(provider);
    }
    private Configuration prepareConf(String providerName){
        Configuration newConf = new Configuration();
        Iterator<Map.Entry<String, String>> entries = conf.iterator();
        String providerKey=MAPPING_PROVIDERS_CONFIG_PREFIX+"."+providerName;
        while (entries.hasNext()) {
            Map.Entry<String, String> entry = entries.next();
            String key = entry.getKey();
            if (key.startsWith(providerKey) && !key.equals(providerKey)) {
                key=key.replace(".provider."+providerName,"");
                newConf.set(key,entry.getValue());
            }
        }
        return newConf;
    }
}
