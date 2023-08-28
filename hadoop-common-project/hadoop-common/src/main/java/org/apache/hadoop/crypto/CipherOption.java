package org.apache.hadoop.crypto;

import com.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
public class CipherOption {
    private final CipherSuite suite;
    private final byte[] inKey;
    private final byte[] inIv;
    private final byte[] outKey;
    private final byte[] outIv;

    public CipherOption(CipherSuite suite){
        this(suite,null,null,null,null);
    }
    public CipherOption(CipherSuite suite,byte[] inKey,byte[] inIv,byte[] outKey,byte[] outIv){
        this.suite=suite;
        this.inKey=inKey;
        this.inIv=inIv;
        this.outIv=outIv;
        this.outKey=outKey;
    }

    public CipherSuite getCipherSuite() {
        return suite;
    }

    public byte[] getInKey() {
        return inKey;
    }

    public byte[] getInIv() {
        return inIv;
    }

    public byte[] getOutKey() {
        return outKey;
    }

    public byte[] getOutIv() {
        return outIv;
    }

}
