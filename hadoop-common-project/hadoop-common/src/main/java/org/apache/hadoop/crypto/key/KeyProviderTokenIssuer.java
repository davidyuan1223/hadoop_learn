package org.apache.hadoop.crypto.key;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.token.DelegationTokenIssuer;

import java.io.IOException;
import java.net.URI;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface KeyProviderTokenIssuer extends DelegationTokenIssuer {
    KeyProvider getKeyProvider()throws IOException;
    URI getKeyProviderUri()throws IOException;
}
