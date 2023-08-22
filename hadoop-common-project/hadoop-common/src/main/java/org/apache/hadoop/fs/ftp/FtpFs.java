package org.apache.hadoop.fs.ftp;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.commons.net.ftp.FTP;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateFileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FtpFs extends DelegateFileSystem {
    FtpFs(final URI theUri, final Configuration conf)throws IOException, URISyntaxException{
        super(theUri,new FTPFileSystem(),conf, FsConstants.FTP_SCHEME,true);
    }
    @Override
    public int getUriDefaultPort(){
        return FTP.DEFAULT_PORT;
    }
    @Override
    @Deprecated
    public FsServerDefaults getServerDefaults()throws IOException{
        return FtpConfigKeys.getServerDefaults();
    }
    @Override
    public FsServerDefaults getServerDefaults(final Path f)throws IOException{
        return FtpConfigKeys.getServerDefaults();
    }
}
