package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.StringInterner;

import java.io.IOException;
import java.io.Serializable;

/**
 * @Description:
 * Represents the network location of a block,information about the hosts
 * that contains block replicas,and other block metadata(E.g. the file
 * offset associated with the block,length,whether the block is corrupt,etc)
 * For a single BlockLocation, it will have different meaning for replicated
 * and erasure coded files.
 * If the file is 3-replicated, offset and length of a BlockLocation represent
 * the absolute value in the file and the hosts are the 3 datanodes that
 * holding the replicas. Here is an example:
 * <pre>
 *     BlockLocation(offset: 0, length: BLOCK_SIZE,
 *     hosts: {"host1:9866","host2:9866","host3:9866"})
 * </pre>
 * And if the file is erasure-coded, each BlockLocation represents a logical
 * block groups. Value offset is the offset of a block group in the file and
 * value length is the total length of a block group. Hosts of a BlockLocation
 * are the datanodes that holding all the data blocks and parity blocks of a
 * block group.
 *
 * @Author: yuan
 * @Date: 2023/07/29
 **/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class BlockLocation implements Serializable {
    private static final long serialVersionUID = 0x22986f6d;
    private String[] hosts;
    private String[] cachedHosts;
    private String[] names;
    private String[] topologyPaths;
    private String[] storageIds;
    private StorageType[] storageTypes;
    private long offset;
    private long length;
    private boolean corrupt;
    private static final String[] EMPTY_STR_ARRAY = new String[0];
    private static final StorageType[] EMPTY_STORAGE_TYPE_ARRAY = StorageType.EMPTY_ARRAY;

    public BlockLocation(){
        this(EMPTY_STR_ARRAY,EMPTY_STR_ARRAY,0L,0L);
    }
    public BlockLocation(BlockLocation that){
        this.hosts=that.hosts;
        this.cachedHosts=that.cachedHosts;
        this.names=that.names;
        this.topologyPaths=that.topologyPaths;
        this.offset = that.offset;
        this.length = that.length;
        this.corrupt = that.corrupt;
        this.storageIds=that.storageIds;
        this.storageTypes=that.storageTypes;
    }

    public BlockLocation(String[] names,String[] hosts,long offset,long length){
        this(names,hosts,offset,length,false);
    }
    public BlockLocation(String[] names,String[] hosts,long offset,long length,boolean corrupt){
        this(names,hosts,null,offset,length,corrupt);
    }
    public BlockLocation(String[] names,String[] hosts,String[] topologyPaths,long offset,long length){
        this(names,hosts,topologyPaths,offset,length,false);
    }
    public BlockLocation(String[] names,String[] hosts,String[] topologyPaths,long offset,long length,boolean corrupt){
        this(names,hosts,null,topologyPaths,offset,length,corrupt);
    }
    public BlockLocation(String[] names,String[] hosts,String[] cachedHosts,String[] topologyPaths,long offset,long length,boolean corrupt){
        this(names,hosts,cachedHosts,topologyPaths,null,null,offset,length,corrupt);
    }

    public BlockLocation(String[] names, String[] hosts, String[] cachedHosts, String[] topologyPaths, String[] storageIds, StorageType[] storageTypes, long offset, long length, boolean corrupt) {
        if (names == null) {
            this.names=EMPTY_STR_ARRAY;
        }else {
            this.names= StringInterner.internStringInArray(names);
        }
        if (hosts == null) {
            this.hosts=EMPTY_STR_ARRAY;
        }else {
            this.hosts= StringInterner.internStringInArray(hosts);
        }
        if (cachedHosts == null) {
            this.cachedHosts=EMPTY_STR_ARRAY;
        }else {
            this.cachedHosts= StringInterner.internStringInArray(cachedHosts);
        }
        if (topologyPaths == null) {
            this.topologyPaths = EMPTY_STR_ARRAY;
        }else {
            this.topologyPaths = StringInterner.internStringInArray(topologyPaths);
        }
        if (storageIds == null) {
            this.storageIds = EMPTY_STR_ARRAY;
        }else {
            this.storageIds = StringInterner.internStringInArray(storageIds);
        }
        this.offset=offset;
        this.length=length;
        this.corrupt=corrupt;
    }

    public String[] getHosts() throws IOException {
        return hosts;
    }

    public String[] getCachedHosts() {
        return cachedHosts;
    }

    public String[] getNames() {
        return names;
    }

    public String[] getTopologyPaths() {
        return topologyPaths;
    }

    public String[] getStorageIds() {
        return storageIds;
    }

    public StorageType[] getStorageTypes() {
        return storageTypes;
    }

    public long getOffset() {
        return offset;
    }

    public long getLength() {
        return length;
    }

    public boolean isCorrupt() {
        return corrupt;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public void setCorrupt(boolean corrupt) {
        this.corrupt = corrupt;
    }

    public void setHosts(String[] hosts) {
        if (hosts == null) {
            this.hosts=EMPTY_STR_ARRAY;
        }else {
            this.hosts=StringInterner.internStringInArray(hosts);
        }
    }

    public void setCachedHosts(String[] cachedHosts) {
        if (cachedHosts == null) {
            this.cachedHosts=null;
        }else {
            this.cachedHosts=StringInterner.internStringInArray(cachedHosts);
        }
    }
    public void setNames(String[] names) throws IOException {
        if (names == null) {
            this.names = EMPTY_STR_ARRAY;
        } else {
            this.names = StringInterner.internStringInArray(names);
        }
    }

    /**
     * Set the network topology paths of the hosts.
     *
     * @param topologyPaths topology paths.
     * @throws IOException If an I/O error occurred.
     */
    public void setTopologyPaths(String[] topologyPaths) throws IOException {
        if (topologyPaths == null) {
            this.topologyPaths = EMPTY_STR_ARRAY;
        } else {
            this.topologyPaths = StringInterner.internStringInArray(topologyPaths);
        }
    }

    public void setStorageIds(String[] storageIds) {
        if (storageIds == null) {
            this.storageIds = EMPTY_STR_ARRAY;
        } else {
            this.storageIds = StringInterner.internStringInArray(storageIds);
        }
    }

    public void setStorageTypes(StorageType[] storageTypes) {
        if (storageTypes == null) {
            this.storageTypes = EMPTY_STORAGE_TYPE_ARRAY;
        } else {
            this.storageTypes = storageTypes;
        }
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append(offset)
                .append(',')
                .append(length);
        if (corrupt) {
            result.append("(corrupt)");
        }
        for(String h: hosts) {
            result.append(',');
            result.append(h);
        }
        return result.toString();
    }
}
