package org.apache.hadoop.crypto.key;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import com.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.DelegationTokenIssuer;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;

public class KeyProviderDelegationTokenExtension extends KeyProviderExtension<KeyProviderDelegationTokenExtension.DelegationTokenExtension> implements DelegationTokenIssuer {
    private static DelegationTokenExtension DEFAULT_EXTENSION=new DefaultDelegationTokenExtension();

    private KeyProviderDelegationTokenExtension(KeyProvider keyProvider,DelegationTokenExtension extension){
        super(keyProvider,extension);
    }

    @Override
    public String getCanonicalServiceName() {
        return getExtension().getCanonicalServiceName();
    }

    @Override
    public Token<?> getDelegationToken(String renewer) throws IOException {
        return getExtension().getDelegationToken(renewer);
    }
    public static KeyProviderDelegationTokenExtension createKeyProviderDelegationTokenExtension(KeyProvider keyProvider){
        DelegationTokenExtension delTokExtension = (keyProvider instanceof DelegationTokenExtension) ? (DelegationTokenExtension) keyProvider : DEFAULT_EXTENSION;
        return new KeyProviderDelegationTokenExtension(keyProvider,delTokExtension);
    }

    public interface DelegationTokenExtension extends KeyProviderExtension.Extension,DelegationTokenIssuer{
        long renewDelegationToken(final Token<?> token)throws IOException;
        Void cancelDelegationToken(final Token<?> token)throws IOException;
        @VisibleForTesting
        @InterfaceAudience.Private
        @InterfaceStability.Unstable
        Token<?> selectDelegationToken(Credentials credentials);
    }

    private static class DefaultDelegationTokenExtension implements DelegationTokenExtension{
        @Override
        public Token<?>[] addDelegationTokens(String renewer, Credentials credentials) throws IOException {
            return null;
        }

        @Override
        public String getCanonicalServiceName() {
            return null;
        }

        @Override
        public Token<?> getDelegationToken(String renewer) throws IOException {
            return null;
        }

        @Override
        public long renewDelegationToken(Token<?> token) throws IOException {
            return 0;
        }

        @Override
        public Void cancelDelegationToken(Token<?> token) throws IOException {
            return null;
        }

        @Override
        public Token<?> selectDelegationToken(Credentials credentials) {
            return null;
        }

    }
}
