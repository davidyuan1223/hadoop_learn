package org.apache.hadoop.fs.audit;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class AuditConstant {
    private AuditConstant(){}
    public static final String REFERRER_ORIGIN_HOST="audit.example.org";
    public static final String PARAM_COMMAND="cm";
    public static final String PARAM_FILESYSTEM_ID="fs";
    public static final String PARAM_ID="id";
    public static final String PARAM_JOB_ID="ji";
    public static final String PARAM_OP="op";
    public static final String PARAM_PATH="p1";
    public static final String PARAM_PATH2="p2";
    public static final String PARAM_PRINCIPAL="pr";
    public static final String PARAM_PROCESS="ps";
    public static final String PARAM_TASK_ATTEMPT_ID="ta";
    public static final String PARAM_THREAD0="t0";
    public static final String PARAM_THREAD1="t1";
    public static final String PARAM_TIMESTAMP="ts";
}
