package org.apache.hadoop.security.authentication.util;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import com.apache.hadoop.classification.VisibleForTesting;

import javax.servlet.ServletContext;
import java.security.SecureRandom;
import java.util.Properties;
import java.util.Random;

@InterfaceStability.Unstable
@InterfaceAudience.Private
public class RandomSignerSecretProvider extends RolloverSignerSecretProvider{
    private final Random random;

    public RandomSignerSecretProvider(){
        super();
        random=new SecureRandom();
    }

    @Override
    protected byte[] generateNewSecret() {
        byte[] secret = new byte[32];
        random.nextBytes(secret);
        return secret;
    }

    @VisibleForTesting
    public RandomSignerSecretProvider(long seed){
        super();
        random=new Random(seed);
    }

}
