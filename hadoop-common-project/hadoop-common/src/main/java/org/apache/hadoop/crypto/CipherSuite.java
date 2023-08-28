package org.apache.hadoop.crypto;

import com.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.util.StringUtils;

@InterfaceAudience.Private
public enum CipherSuite {
    UNKNOWN("Unknown",0),
    AES_CTR_NOPADDIND("AES/CTR/NoPadding",16),
    SM4_CTR_NOPADDING("SM4/CTR/NoPadding",16);

    private final String name;
    private final int algoBlockSize;
    private Integer unknownValue=null;

    CipherSuite(String name,int algoBlockSize){
        this.name=name;
        this.algoBlockSize=algoBlockSize;
    }
    public void setUnknownValue(Integer unknownValue) {
        this.unknownValue = unknownValue;
    }

    public Integer getUnknownValue() {
        return unknownValue;
    }

    public String getName() {
        return name;
    }

    public int getAlgoBlockSize() {
        return algoBlockSize;
    }
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("{");
        builder.append("name: " + name)
                .append(", algorithmBlockSize: " + algoBlockSize);
        if (unknownValue != null) {
            builder.append(", unknownValue: " + unknownValue);
        }
        builder.append("}");
        return builder.toString();
    }
    public static CipherSuite convert(String name){
        CipherSuite[] suites = CipherSuite.values();
        for (CipherSuite suite : suites) {
            if (suite.getName().equals(name)) {
                return suite;
            }
        }
        throw new IllegalArgumentException("Invalid cipher suite name: "+name);
    }

    public String getConfigSuffix(){
        String[] parts = name.split("/");
        StringBuilder suffix = new StringBuilder();
        for (String part : parts) {
            suffix.append(".")
                    .append(StringUtils.toLowerCase(part));
        }
        return suffix.toString();
    }
}
