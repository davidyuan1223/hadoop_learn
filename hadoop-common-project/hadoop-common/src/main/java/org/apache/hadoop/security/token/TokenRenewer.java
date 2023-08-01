package org.apache.hadoop.security.token;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import java.io.IOException;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class TokenRenewer {
    public abstract boolean handleKind(Text kind);
    public abstract boolean isManaged(Token<?> token)throws IOException;
    public abstract long renew(Token<?> token, Configuration conf)throws IOException,InterruptedException;
    public abstract void cancel(Token<?> token,Configuration conf)throws IOException,InterruptedException;

}
