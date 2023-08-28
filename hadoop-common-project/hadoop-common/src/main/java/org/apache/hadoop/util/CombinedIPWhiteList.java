package org.apache.hadoop.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CombinedIPWhiteList implements IPList{
    private static final Logger LOG= LoggerFactory.getLogger(CombinedIPWhiteList.class);
    private static final String LOCALHOST_IP="127.0.0.1";
    private final IPList[] ipWhiteList;
    public CombinedIPWhiteList(String fixedWhiteListFile,
                               String variableWhiteListFile,
                               long cacheExpiryInSeconds) {
        IPList fixedIpList = new FileBasedIPList(fixedWhiteListFile);
        if (variableWhiteListFile != null) {
            IPList variableIpList = new CacheableIPList(new FileBasedIPList(variableWhiteListFile), cacheExpiryInSeconds);
            ipWhiteList=new IPList[]{fixedIpList, variableIpList};
        } else {
            ipWhiteList=new IPList[]{fixedIpList};
        }
    }

    @Override
    public boolean isIn(String ipAddress) {
        if (ipAddress == null) {
            throw new IllegalArgumentException("ipAddress cannot be null");
        }
        if (LOCALHOST_IP.equals(ipAddress)) {
            return true;
        }
        for (IPList ipList : ipWhiteList) {
            if (ipList.isIn(ipAddress)) {
                return true;
            }
        }
        return false;
    }
}
