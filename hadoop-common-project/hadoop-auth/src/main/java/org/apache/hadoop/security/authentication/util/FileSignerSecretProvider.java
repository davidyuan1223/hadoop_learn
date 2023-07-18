package org.apache.hadoop.security.authentication.util;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import javax.servlet.ServletContext;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
@InterfaceStability.Unstable
@InterfaceAudience.Private
public class FileSignerSecretProvider extends SignerSecretProvider {
    private byte[] secret;
    private byte[][] secrets;
    public FileSignerSecretProvider(){}
    @Override
    public void init(Properties config, ServletContext servletContext, long tokenValidity) throws Exception {
        String signatureSecretFile = config.getProperty(AuthenticationFilter.SIGNATURE_SECRET_FILE, null);
        if (signatureSecretFile != null) {
            try(Reader reader=new InputStreamReader(Files.newInputStream(
                    Paths.get(signatureSecretFile)), StandardCharsets.UTF_8)){
                StringBuilder sb = new StringBuilder();
                int c = reader.read();
                while (c > -1) {
                    sb.append((char) c);
                    c= reader.read();
                }
                secret=sb.toString().getBytes(StandardCharsets.UTF_8);
                if (secret.length==0) {
                    throw new RuntimeException("No secret in signature secret file: "+signatureSecretFile);
                }
            }catch (IndexOutOfBoundsException e){
                throw new RuntimeException("Could not read signature secret file: "+signatureSecretFile);
            }
        }
        secrets=new byte[][]{secret};
    }

    @Override
    public byte[] getCurrentSecret() {
        return secret;
    }

    @Override
    public byte[][] getAllSecrets() {
        return secrets;
    }
}
