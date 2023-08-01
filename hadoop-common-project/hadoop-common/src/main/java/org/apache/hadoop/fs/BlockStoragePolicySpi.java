package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

/**
 * @Description:
 * A storage policy specifies the placement of block replicas on specific
 * storage types.
 * @Author: yuan
 * @Date: 2023/07/30
 **/
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface BlockStoragePolicySpi {
    /**
     * Return the name of the storage policy. Policies are uniquely
     * identified by name.
     * @return the name of the storage policy
     */
    String getName();

    /**
     * Return the preferred storage types associated with this policy. These
     * storage types are used sequentially for successive block replicas.
     * @return preferred storage types used for subsequent replicas
     */
    StorageType[] getStorageTypes();


}
