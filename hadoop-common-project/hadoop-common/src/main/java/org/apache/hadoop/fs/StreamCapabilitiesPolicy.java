package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class StreamCapabilitiesPolicy {
    public static final String CAN_UNBUFFFER_NOT_IMPLEMENTED_MESSAGE=
            "claims unbuffer capability but does not implement CanUnbuffer";
    static final Logger LOG= LoggerFactory.getLogger(StreamCapabilitiesPolicy.class);

    public static void unbuffer(InputStream in){
        try {
            if (in instanceof StreamCapabilities
                    && ((StreamCapabilities) in).hasCapability(StreamCapabilities.UNBUFFER)) {
                ((CanUnbuffer)in).unbuffer();
            }else {
                LOG.debug(in.getClass().getName()+":"+
                        " does not implement StreamCapabilities" +
                        " and the unbuffer capability");
            }
        }catch (ClassCastException c){
            throw new UnsupportedOperationException(in.getClass().getName()+":" +
                    CAN_UNBUFFFER_NOT_IMPLEMENTED_MESSAGE);
        }
    }
}
