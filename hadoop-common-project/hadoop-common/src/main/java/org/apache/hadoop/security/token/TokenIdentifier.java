package org.apache.hadoop.security.token;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.Arrays;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class TokenIdentifier implements Writable {
    private String trackingId=null;
    public abstract Text getKind();
    public abstract UserGroupInformation getUser();
    public byte[] getBytes(){
        DataOutputBuffer buf = new DataOutputBuffer(4096);
        try{
            this.writer(buf);
        }catch (IOException e){
            throw new RuntimeException("io error in getBytes",e);
        }
        return Arrays.copyOf(buf.getData(),buf.getLength());
    }
    public String getTrackingId(){
        if (trackingId == null) {
            trackingId= DigestUtils.md5Hex(getBytes());
        }
        return trackingId;
    }
}
