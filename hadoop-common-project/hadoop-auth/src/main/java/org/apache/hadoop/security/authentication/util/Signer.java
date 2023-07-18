package org.apache.hadoop.security.authentication.util;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Signer {
    private static final String SIGNATURE="&s=";
    private static final String SIGNING_ALGORITHM="HmacSHA256";
    private SignerSecretProvider secretProvider;

    public Signer(SignerSecretProvider secretProvider){
        if (secretProvider == null) {
            throw new IllegalArgumentException("secretProvider cannot be NULL");
        }
        this.secretProvider=secretProvider;
    }

    public synchronized String sign(String str){
        if (str == null || str.length() == 0) {
            throw new IllegalArgumentException("NULL or empty string to sign");
        }
        byte[] currentSecret = secretProvider.getCurrentSecret();
        String signature=computeSignature(currentSecret,str);
        return str+SIGNATURE+signature;
    }

    protected String computeSignature(byte[] secret, String str) {
        try {
            SecretKeySpec key = new SecretKeySpec((secret), SIGNING_ALGORITHM);
            Mac mac = Mac.getInstance(SIGNING_ALGORITHM);
            mac.init(key);
            byte[] sig = mac.doFinal(StringUtils.getBytesUtf8(str));
            return new Base64(0).encodeToString(sig);
        }catch (NoSuchAlgorithmException | InvalidKeyException e){
            throw new RuntimeException("It should not happen, "+e.getMessage(),e);
        }
    }

    public String verifyAndExtract(String signedStr) throws SignerException {
        int index = signedStr.lastIndexOf(SIGNATURE);
        if (index==-1) {
            throw new SignerException("Invalid signed text: "+signedStr);
        }
        String originalSignature = signedStr.substring(index + SIGNATURE.length());
        String rawValue = signedStr.substring(0, index);
        checkSignatures(rawValue,originalSignature);
        return rawValue;
    }

    protected void checkSignatures(String rawValue, String originalSignature) throws SignerException {
        byte[] originalSignatureBytes = StringUtils.getBytesUtf8(originalSignature);
        boolean isValid=false;
        byte[][] secrets = secretProvider.getAllSecrets();
        for (byte[] secret : secrets) {
            if (secret != null) {
                String currentSignature = computeSignature(secret, rawValue);
                if (MessageDigest.isEqual(originalSignatureBytes,
                        StringUtils.getBytesUtf8(currentSignature))) {
                    isValid = true;
                    break;
                }
            }
        }
        if (!isValid) {
            throw new SignerException("Invalid signature");
        }
    }
}
