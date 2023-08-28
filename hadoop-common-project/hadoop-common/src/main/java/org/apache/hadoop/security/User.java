package org.apache.hadoop.security;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.security.Principal;
@InterfaceAudience.LimitedPrivate({"HDFS","MapReduce"})
@InterfaceStability.Evolving
public class User implements Principal {
    private final String fullName;
    private final String shortName;
    private volatile AuthenticationMethod authMethod=null;
}
