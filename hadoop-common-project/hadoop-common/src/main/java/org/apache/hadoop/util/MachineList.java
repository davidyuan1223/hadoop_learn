package org.apache.hadoop.util;

import com.apache.hadoop.classification.VisibleForTesting;
import org.apache.commons.net.util.SubnetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public class MachineList {
    public static final Logger LOG= LoggerFactory.getLogger(MachineList.class);
    public static final String WILDCARD_VALUE="*";
    public static class InetAddressFactory{
        static final InetAddressFactory S_INSTANCE=new InetAddressFactory();
        public InetAddress getByName(String host)throws UnknownHostException{
            return InetAddress.getByName(host);
        }
    }
    private final boolean all;
    private final Set<InetAddress> inetAddresses;
    private final Collection<String > entries;
    private final List<SubnetUtils.SubnetInfo> cidrAddresses;
    private final InetAddressFactory addressFactory;

    public MachineList(String hostEntries){
        this(hostEntries,InetAddressFactory.S_INSTANCE);
    }
    public MachineList(String hostEntries,InetAddressFactory addressFactory){
        this(StringUtils.getTrimmedStringCollection(hostEntries),addressFactory);
    }
    public MachineList(Collection<String > hostEntries){
        this(hostEntries,InetAddressFactory.S_INSTANCE);
    }
    public MachineList(Collection<String > hostEntries,InetAddressFactory addressFactory){
        this.addressFactory=addressFactory;
        if (hostEntries != null) {
            entries=new ArrayList<>(hostEntries);
            if ((hostEntries.size() == 1) && (hostEntries.contains(WILDCARD_VALUE))) {
                all=true;
                inetAddresses=null;
                cidrAddresses=null;
            }else {
                all=false;
                Set<InetAddress> addrs=new HashSet<>();
                List<SubnetUtils.SubnetInfo> cidrs=new LinkedList<>();
                for (String hostEntry : hostEntries) {
                    if (hostEntry.contains("/")) {
                        try {
                            SubnetUtils subnet = new SubnetUtils(hostEntry);
                            subnet.setInclusiveHostCount(true);
                            cidrs.add(subnet.getInfo());
                        }catch (IllegalArgumentException e){
                            LOG.warn("Invalid CIDR syntax : "+hostEntry);
                            throw e;
                        }
                    }else {
                        try {
                            addrs.add(addressFactory.getByName(hostEntry));
                        }catch (UnknownHostException e){
                            LOG.warn(e.toString());
                        }
                    }
                }
                inetAddresses=(addrs.size()>0)?addrs:null;
                cidrAddresses=(cidrs.size()>0)?cidrs:null;
            }
        }else {
            all=false;
            inetAddresses=null;
            cidrAddresses=null;
            entries=Collections.emptyList();
        }
    }
    public boolean includes(String ipAddress){
        if (all) {
            return true;
        }
        if (ipAddress == null) {
            throw new IllegalArgumentException("ipAddress is null");
        }
        try {
            return includes(addressFactory.getByName(ipAddress));
        }catch (UnknownHostException e){
            return false;
        }
    }
    public boolean includes(InetAddress address){
        if (all) {
            return true;
        }
        if (address == null) {
            throw new IllegalArgumentException("address is null.");
        }
        if (inetAddresses != null && inetAddresses.contains(address)) {
            return true;
        }
        if (cidrAddresses != null) {
            String ipAddress = address.getHostAddress();
            for (SubnetUtils.SubnetInfo cidrAddress : cidrAddresses) {
                if (cidrAddress.isInRange(ipAddress)) {
                    return true;
                }
            }
        }
        return false;
    }
    @VisibleForTesting
    public Collection<String > getCollection(){
        return entries;
    }
}
