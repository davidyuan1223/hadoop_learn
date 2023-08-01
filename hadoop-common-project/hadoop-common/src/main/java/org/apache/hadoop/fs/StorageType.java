package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Defines the types of supported storage media. The default storage medium
 * is assumed to be DISK
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public enum StorageType {
    RAM_DISK(true,true),
    SSD(false,false),
    DISK(false,false),
    ARCHIVE(false,false),
    PROVIDED(false,false),
    NVDIMM(false,true);

    private final boolean isTransient;
    private final boolean isRAM;
    public static final StorageType DEFAULT=DISK;
    public static final StorageType[] EMPTY_ARRAY={};
    public static final StorageType[] VALUES=values();

    StorageType(boolean isTransient, boolean isRAM) {
        this.isTransient = isTransient;
        this.isRAM = isRAM;
    }
    public boolean isTransient() {
        return isTransient;
    }
    public boolean isRAM() {
        return isRAM;
    }
    public boolean supportTypeQuota(){
        return !isTransient;
    }

    public boolean isMovable(){
        return !isTransient;
    }
    public static List<StorageType> asList(){
        return Arrays.asList(VALUES);
    }
    public static List<StorageType> getMovableList(){
        return getNonTransientTypes();
    }

    private static List<StorageType> getNonTransientTypes() {
        List<StorageType> nonTransientTypes = new ArrayList<StorageType>();
        for (StorageType storageType : VALUES) {
            if (!storageType.isTransient()) {
                nonTransientTypes.add(storageType);
            }
        }
        return nonTransientTypes;
    }
    public static List<StorageType> getTypesSupportingQuota(){
        return getNonTransientTypes();
    }
    public static StorageType parseStorageType(int i){
        return VALUES[i];
    }
    public static StorageType parseStorageType(String s){
        return StorageType.valueOf(StringUtils.toUpperCase(s));
    }
    public static boolean allowSameDiskTiering(StorageType storageType){
        return storageType==DISK || storageType==ARCHIVE;
    }
    public static final String CONF_KEY_HEADER="dfs.datanode.storagetype.";
    public static String getConf(Configuration conf,StorageType t,String name){
        return conf.get(CONF_KEY_HEADER+t.toString()+"."+name);
    }

}
