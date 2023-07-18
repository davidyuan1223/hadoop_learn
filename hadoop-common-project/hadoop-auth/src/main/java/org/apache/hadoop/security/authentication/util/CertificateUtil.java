package org.apache.hadoop.security.authentication.util;

import javax.servlet.ServletException;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.PublicKey;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPublicKey;

public class CertificateUtil {
    private static final String PEM_HEADER = "-----BEGIN CERTIFICATE-----\n";
    private static final String PEM_FOOTER = "\n-----END CERTIFICATE-----";

    /**
     * Gets an RSAPublicKey from the provided PEM encoding.
     *
     * @param pem
     *          - the pem encoding from config without the header and footer
     * @return RSAPublicKey the RSA public key
     * @throws ServletException thrown if a processing error occurred
     */
    public static RSAPublicKey parseRSAPublicKey(String pem) throws ServletException {
        String fullPem = PEM_HEADER + pem + PEM_FOOTER;
        PublicKey key = null;
        try {
            CertificateFactory fact = CertificateFactory.getInstance("X.509");
            ByteArrayInputStream is = new ByteArrayInputStream(
                    fullPem.getBytes(StandardCharsets.UTF_8));

            X509Certificate cer = (X509Certificate) fact.generateCertificate(is);
            key = cer.getPublicKey();
        } catch (CertificateException ce) {
            String message = null;
            if (pem.startsWith(PEM_HEADER)) {
                message = "CertificateException - be sure not to include PEM header "
                        + "and footer in the PEM configuration element.";
            } else {
                message = "CertificateException - PEM may be corrupt";
            }
            throw new ServletException(message, ce);
        }
        return (RSAPublicKey) key;
    }
}
