package org.apache.hadoop.security.token;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.LimitedPrivate({"HDFS","MapReduce","Yarn"})
@InterfaceStability.Unstable
public interface DelegationTokenIssuer {
    Logger TOKEN_LOG= LoggerFactory.getLogger(DelegationTokenIssuer.class);
    String getCanonicalServiceName();
    Token
}
