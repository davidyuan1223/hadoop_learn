package org.apache.hadoop.fs.local;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumFs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class LocalFs extends ChecksumFs {
    LocalFs(final Configuration conf)throws IOException, URISyntaxException{
        super(new RawLocalFs(conf));
    }
    LocalFs(final URI theUri,final Configuration conf)throws IOException,URISyntaxException{
        this(conf);
    }
}
