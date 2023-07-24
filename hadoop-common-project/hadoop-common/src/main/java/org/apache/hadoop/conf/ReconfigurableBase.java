package org.apache.hadoop.conf;

import com.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/22
 **/
public abstract class ReconfigurableBase extends Configured implements ReConfigurable{
    private static final Logger LOG= LoggerFactory.getLogger(ReconfigurableBase.class);
    private ReconfigurationUtil reconfigurationUtil=new ReconfigurationUtil();
    private Thread reconfigThread=null;
    private volatile boolean shouldRun=true;
    private Object reconfigLock=new Object();
    private long startTime=0;
    private long endTime=0;
    private Map<ReconfigurationUtil.PropertyChange, Optional<String >> status=null;

    public ReconfigurableBase(){super(new Configuration());}
    public ReconfigurableBase(Configuration conf){
        super((conf==null)?new Configuration():conf);
    }
    @VisibleForTesting
    public void setReconfigurationUtil(ReconfigurationUtil ru){
        reconfigurationUtil= Preconditions.checkNotNull(ru);
    }
    protected abstract Configuration getNewConf();
    @VisibleForTesting
    public Collection<ReconfigurationUtil.PropertyChange> getChangedProperties(Configuration newConf,
                                                                               Configuration oldConf){
        return reconfigurationUtil.parseChangedProperties(newConf,oldConf);
    }


    private static class ReconfigurationThread extends Thread{
        private ReconfigurableBase parent;
        ReconfigurationThread(ReconfigurableBase base){
            this.parent=base;
        }

        @Override
        public void run() {
            LOG.info("Starting reconfiguration task.");
            final Configuration oldConf=parent.getConf();
            final Configuration newConf=parent.getNewConf();
            final Collection<ReconfigurationUtil.PropertyChange> changes=
                    parent.getChangedProperties(newConf,oldConf);
            Map<ReconfigurationUtil.PropertyChange,Optional<String >> results=
                    Maps.newHashMap();
            ConfigRedactor oldRedactor=new ConfigRedactor(oldConf);
            ConfigRedactor newRedactor=new ConfigRedactor(newConf);
            for (ReconfigurationUtil.PropertyChange change : changes) {
                String errorMessage=null;
                String oldValRedacted=oldRedactor.redact(change.prop,change.oldVal);
                String newValRedacted=newRedactor.redact(change.prop,change.newVal);
                if (!parent.isPropertyReconfigurable(change.prop)) {
                    LOG.info(String.format(
                            "Property %s is not configurable: old value: %s, new Value: %s",
                            change.prop,
                            oldValRedacted,
                            newValRedacted
                    ));
                    continue;
                }
                LOG.info("Change property: "+change.prop+" from \""
                +((change.oldVal==null)?"<default>":oldValRedacted)
                +"\" to \""
                +((change.newVal==null)?"<default>":newValRedacted)
                +"\".");
                try {
                    String effectiveValue=
                            parent.reconfigurePropertyImpl(change.prop,change.newVal);
                    if (change.newVal != null) {
                        oldConf.set(change.prop,effectiveValue);
                    }else {
                        oldConf.unset(change.prop);
                    }
                }catch (ReconfigurationException e){
                    Throwable cause=e.getCause();
                    errorMessage=cause==null?e.getMessage():cause.getMessage();
                }
                results.put(change,Optional.ofNullable(errorMessage));
            }
            synchronized (parent.reconfigLock){
                parent.endTime= Time.now();
                parent.status= Collections.unmodifiableMap(results);
                parent.reconfigThread=null;
            }
        }
    }
    public void startReconfigurationTask() throws IOException {
        synchronized (reconfigLock){
            if (!shouldRun) {
                String errorMessage="The server is stopped.";
                LOG.warn(errorMessage);
                throw new IOException(errorMessage);
            }
            if (reconfigThread != null) {
                String errorMessage="Another reconfiguration task is running.";
                LOG.warn(errorMessage);
                throw new IOException(errorMessage);
            }
            reconfigThread=new ReconfigurationThread(this);
            reconfigThread.setDaemon(true);
            reconfigThread.setName("Reconfiguration Task");
            reconfigThread.start();
            startTime=Time.now();
        }
    }

    public ReconfigurationTaskStatus getReconfigurationTaskStatus(){
        synchronized (reconfigLock){
            if (reconfigThread != null) {
                return new ReconfigurationTaskStatus(startTime,0,null);
            }
            return new ReconfigurationTaskStatus(startTime,endTime,status);
        }
    }

    public void shutdownReconfigurationTask(){
        Thread tempThread;
        synchronized (reconfigLock){
            shouldRun=false;
            if (reconfigThread == null) {
                return;
            }
            tempThread=reconfigThread;
            reconfigThread=null;
        }
        try {
            tempThread.join();
        }catch (InterruptedException e){

        }
    }

    @Override
    public void reconfigureProperty(String property, String newVal) throws ReconfigurationException {
        if (isPropertyReconfigurable(property)) {
            LOG.info("changing property "+property+" to "+newVal);
            synchronized (getConf()){
                getConf().get(property);
                String effectiveValue=reconfigurePropertyImpl(property,newVal);
                if (newVal != null) {
                    getConf().set(property,effectiveValue);
                }else {
                    getConf().unset(property);
                }
            }
        }else {
            throw new ReconfigurationException(property,newVal,getConf().get(property));
        }
    }

    protected abstract String reconfigurePropertyImpl(
            String property,String newVal
    )throws ReconfigurationException;

    @Override
    public boolean isPropertyReconfigurable(String property) {
        return getReconfigurableProperties().contains(property);
    }

    @Override
    public abstract Collection<String> getReconfigurableProperties() ;

}
