package org.apache.hadoop.util;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;

import java.util.function.Supplier;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public class DurationInfo extends OperationDuration implements AutoCloseable{
    private final Supplier<String > text;
    private String textStr;
    private final Logger log;
    private final boolean logAtInfo;
    public DurationInfo(Logger log,String format,Object... args){
        this(log,true,format,args);
    }
    public DurationInfo(Logger log,boolean logAtInfo,String format,Object... args){
        this.text=()->String.format(format,args);
        this.log=log;
        this.logAtInfo=logAtInfo;
        if (logAtInfo) {
            log.info("Starting: {}",getFormattedText());
        }else {
            if (log.isDebugEnabled()) {
                log.debug("Starting: {}",getFormattedText());
            }
        }
    }
    private String getFormattedText(){
        return textStr==null?(textStr=text.get()):textStr;
    }
    @Override
    public String toString() {
        return getFormattedText() + ": duration " + super.toString();
    }

    @Override
    public void close() {
        finished();
        if (logAtInfo) {
            log.info("{}", this);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("{}", this);
            }
        }
    }

}
