package org.apache.hadoop.security.token;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class TokenIdentifier implements Writable {
    private String trackingId=null;
    public abstract Text
}
