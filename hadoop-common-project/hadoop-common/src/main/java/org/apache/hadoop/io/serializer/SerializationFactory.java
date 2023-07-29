package org.apache.hadoop.io.serializer;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configured;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.LimitedPrivate({"HDFS","MapReduce"})
@InterfaceStability.Evolving
public class SerializationFactory extends Configured {
    static final Logger LOG= LoggerFactory.getLogger(SerializationFactory.class.getName());

}
