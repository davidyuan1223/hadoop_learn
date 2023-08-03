package org.apache.hadoop.security.token;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@InterfaceAudience.LimitedPrivate({"HDFS","MapReduce","Yarn"})
@InterfaceStability.Unstable
public interface DelegationTokenIssuer {
    Logger TOKEN_LOG= LoggerFactory.getLogger(DelegationTokenIssuer.class);
    String getCanonicalServiceName();
    Token<?> getDelegationToken(String renewer)throws IOException;
    default DelegationTokenIssuer[] getAdditionalTokenIssuers()throws IOException{
        return null;
    }
    default Token<?>[] addDelegationTokens(final String renewer, Credentials credentials)throws IOException{
        if (credentials == null) {
            credentials=new Credentials();
        }
        final List<Token<?>> tokens=new ArrayList<>();
        collectDelegationTokens(this,renewer,credentials,tokens);
        return tokens.toArray(new Token<?>[tokens.size()]);
    }
    @InterfaceAudience.Private
    static void collectDelegationTokens(final DelegationTokenIssuer issuer,final String renewer,
                                        final Credentials credentials,final List<Token<?>> tokens)throws IOException{
        final String serviceName = issuer.getCanonicalServiceName();
        if (TOKEN_LOG.isDebugEnabled()) {
            TOKEN_LOG.debug("Search token for service {} in credentials",serviceName);
        }
        if (serviceName != null) {
            final Text service=new Text(serviceName);
            Token<?> token = credentials.getToken(service);
            if (token == null) {
                if (TOKEN_LOG.isDebugEnabled()) {
                    TOKEN_LOG.debug("Token for service {} not found in credentials,try getDelegationToken.",serviceName);
                }
                token=issuer.getDelegationToken(renewer);
                if (token != null) {
                    tokens.add(token);
                    credentials.addToken(service,token);
                }
            }else {
                if (TOKEN_LOG.isDebugEnabled()) {
                    TOKEN_LOG.debug("Token for service {} found in credentials, skip getDelegationToken.",serviceName);
                }
            }
        }
        final DelegationTokenIssuer[] ancillary=issuer.getAdditionalTokenIssuers();
        if (ancillary != null) {
            for (DelegationTokenIssuer subIssuer : ancillary) {
                collectDelegationTokens(subIssuer,renewer,credentials,tokens);
            }
        }
    }
}
