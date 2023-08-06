package org.apache.hadoop.util;

public class CacheableIPList implements IPList{
    private final long cacheTimeout;
    private volatile long cacheExpiryTimeStamp;
    private volatile FileBasedIPList ipList;

    public CacheableIPList(FileBasedIPList ipList,long cacheTimeout){
        this.cacheTimeout=cacheTimeout;
        this.ipList=ipList;
        updateCacheExpiryTime();
    }

    private void reset(){
        ipList=ipList.reload();
        updateCacheExpiryTime();
    }

    private void updateCacheExpiryTime(){
        if (cacheTimeout < 0) {
            cacheExpiryTimeStamp=-1;
        }else {
            cacheExpiryTimeStamp=System.currentTimeMillis()+cacheTimeout;
        }
    }

    public void refresh(){
        cacheExpiryTimeStamp=0;
    }

    @Override
    public boolean isIn(String ipAddress) {
        if (cacheExpiryTimeStamp >= 0 && cacheExpiryTimeStamp < System.currentTimeMillis()) {
            synchronized (this){
                if (cacheExpiryTimeStamp < System.currentTimeMillis()) {
                    reset();
                }
            }
        }
        return ipList.isIn(ipAddress);
    }
}
