package org.apache.hadoop.fs.local;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RawLocalFs extends DelegateToFileSystem {
    RawLocalFs(final Configuration conf)throws IOException, URISyntaxException{
        this(FsConstants.LOCAL_FS_URI,conf);
    }
    RawLocalFs(final URI theUri,final Configuration conf)throws IOException,URISyntaxException{
        super(theUri,new RawLocalFileSystem(),conf,FsConstants.LOCAL_FS_URI.getScheme(),false);
    }
    @Override
    public int getUriDefaultPort(){
        return -1;
    }
    @Override
    public FsServerDefaults getServerDefaults(final Path f)throws IOException{
        return LocalConfigKeys.getServerDefaults();
    }
    @Override
    @Deprecated
    public FsServerDefaults getServerDefaults() throws IOException {
        return LocalConfigKeys.getServerDefaults();
    }

    @Override
    public boolean isValidName(String src) {
        // Different local file systems have different validation rules. Skip
        // validation here and just let the OS handle it. This is consistent with
        // RawLocalFileSystem.
        return true;
    }
}
