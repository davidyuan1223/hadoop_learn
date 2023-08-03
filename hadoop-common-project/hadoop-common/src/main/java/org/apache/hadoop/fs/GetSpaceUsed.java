package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public interface GetSpaceUsed {
    long getUsed() throws IOException;

    class Builder{
        static final Logger LOG= LoggerFactory.getLogger(Builder.class);
        private Configuration conf;
        private Class<? extends GetSpaceUsed> klass=null;
        private File path=null;
        private Long interval=null;
        private Long jitter=null;
        private Long initialUsed=null;
        private Constructor<? extends GetSpaceUsed> cons;

        public Configuration getConf(){
            return conf;
        }
        public Builder setConf(Configuration conf){
            this.conf=conf;
            return this;
        }

        public long getInterval() {
            if (interval != null) {
                return interval;
            }
            long result=CommonConfigurationKeysPublic.FS_DU_INTERVAL_DEFAULT;
            if (conf == null) {
                return result;
            }
            return conf.getLong(CommonConfigurationKeysPublic.FS_DU_INTERVAL_KEY,result);
        }
        public Builder setInterval(long interval){
            this.interval=interval;
            return this;
        }
        public Class<? extends GetSpaceUsed> getKlass(){
            if (klass != null) {
                return klass;
            }
            Class<? extends GetSpaceUsed> result=null;
            if (Shell.WINDOWS) {
                result=WindowsGetSpaceUsed.class;
            }else {
                result=DU.class;
            }
            if (conf == null) {
                return result;
            }
            return conf.getClass(CommonConfigurationKeys.FS_GETSPACEUSED_CLASSNAME,result,GetSpaceUsed.class);
        }

        public Builder setKlass(Class<? extends GetSpaceUsed> klass) {
            this.klass = klass;
            return this;
        }

        public File getPath() {
            return path;
        }

        public Builder setPath(File path) {
            this.path = path;
            return this;
        }

        public long getInitialUsed(){
            if (initialUsed == null) {
                return -1;
            }
            return initialUsed;
        }
        public Builder setInitialUsed(long initialUsed){
            this.initialUsed=initialUsed;
            return this;
        }
        public long getJitter(){
            if (jitter == null) {
                Configuration configuration = this.conf;
                if (configuration == null) {
                    return CommonConfigurationKeys.FS_GETSPACEUSED_JITTER_DEFAULT;
                }
                return configuration.getLong(CommonConfigurationKeys.FS_GETSPACEUSED_JITTER_KEY,
                        CommonConfigurationKeys.FS_GETSPACEUSED_JITTER_DEFAULT);
            }
            return jitter;
        }

        public Builder setJitter(Long jit){
            this.jitter=jit;
            return this;
        }

        public Constructor<? extends GetSpaceUsed> getCons(){
            return cons;
        }

        public void setCons(Constructor<? extends GetSpaceUsed> cons){
            this.cons=cons;
        }
        public GetSpaceUsed build() throws IOException{
            GetSpaceUsed getSpaceUsed=null;
            try {
                if (cons == null) {
                    cons=getKlass().getConstructor(Builder.class);
                }
                getSpaceUsed=cons.newInstance(this);
            }catch (InstantiationException e){
                LOG.warn("Error trying to create an instance of "+getKlass(),e);
            }catch (IllegalAccessException | InvocationTargetException e){
                LOG.warn("Error trying to create "+getKlass(),e);
            }catch (NoSuchMethodException e){
                LOG.warn("Doesn't look like the class "+getKlass()+" have the needed constructor",e);
            }
            if (getSpaceUsed == null) {
                if (Shell.WINDOWS) {
                    getSpaceUsed=new WindowsGetSpaceUsed(this);
                }else {
                    getSpaceUsed=new DU(this);
                }
            }
            if (getSpaceUsed instanceof CachingGetSpaceUsed) {
                ((CachingGetSpaceUsed)getSpaceUsed).init();
            }
            return getSpaceUsed;
        }
    }
}
