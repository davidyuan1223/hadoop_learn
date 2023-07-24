package org.apache.hadoop.conf;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.util.Map;
import java.util.Optional;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/22
 **/
@InterfaceAudience.LimitedPrivate({"HDFS","Management Tools"})
@InterfaceStability.Unstable
public class ReconfigurationTaskStatus {
    long startTime;
    long endTime;
    final Map<ReconfigurationUtil.PropertyChange, Optional<String >> status;

    public ReconfigurationTaskStatus(long startTime, long endTime, Map<ReconfigurationUtil.PropertyChange, Optional<String>> status) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.status = status;
    }
    public boolean hasTask(){
        return startTime>0;
    }
    public boolean stopped(){
        return endTime>0;
    }
    public long getStartTime(){
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public final Map<ReconfigurationUtil.PropertyChange, Optional<String>> getStatus() {
        return status;
    }
}
