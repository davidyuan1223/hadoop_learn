package org.apache.hadoop.fs.permission;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Stable
public enum AclEntryType {
    USER,
    GROUP,
    MASK,
    OTHER;

    @Override
    @InterfaceStability.Unstable
    public String toString() {
        return toStringStable();
    }
    public String toStringStable(){
        return super.toString();
    }
}
