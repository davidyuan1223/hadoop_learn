package org.apache.hadoop.util;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.time.Duration;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public class OperationDuration {
    private final long started;
    private long finished;
    public OperationDuration(){
        started=time();
        finished=started;
    }
    protected long time(){
        return System.currentTimeMillis();
    }
    public void finished(){
        finished=time();
    }
    public String getDurationString(){
        return humanTime(value());
    }
    public static String humanTime(long time){
        long seconds=time/1000;
        long minutes=seconds/60;
        return String.format("%d:%02d.%03ds",minutes,seconds%60,time%1000);
    }

    @Override
    public String toString() {
        return getDurationString();
    }
    public long value(){
        return finished-started;
    }
    public Duration asDuration(){
        return Duration.ofMillis(value());
    }
}
