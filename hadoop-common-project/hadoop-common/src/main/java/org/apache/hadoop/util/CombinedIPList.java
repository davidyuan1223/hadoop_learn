package org.apache.hadoop.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CombinedIPList implements IPList{
    public static final Logger LOG = LoggerFactory.getLogger(CombinedIPList.class);
    private final IPList[] ipLists;
    public CombinedIPList(String fixedBlackListFile,
                          String variableBlackListFile,
                          long cacheExpiryInSeconds){
        IPList fixedNetworkList = new FileBasedIPList(fixedBlackListFile);
        if (variableBlackListFile != null) {
            IPList variableNetworkList = new CacheableIPList(new FileBasedIPList(variableBlackListFile), cacheExpiryInSeconds);
            ipLists=new IPList[]{fixedNetworkList,variableNetworkList};
        }else {
            ipLists=new IPList[]{fixedNetworkList};
        }
    }

    @Override
    public boolean isIn(String ipAddress) {
        if (ipAddress == null) {
            throw new IllegalArgumentException("ip Address cannot be null");
        }
        for (IPList ipList : ipLists) {
            if (ipList.isIn(ipAddress)) {
                return true;
            }
        }
        return false;
    }

}
