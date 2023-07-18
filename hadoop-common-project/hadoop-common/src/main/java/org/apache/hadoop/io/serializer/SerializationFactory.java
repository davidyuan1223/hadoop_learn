package org.apache.hadoop.io.serializer;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

@InterfaceAudience.LimitedPrivate({"HDFS","MapReduce"})
@InterfaceStability.Evolving
public class SerializationFactory extends Configured {
    static final Logger LOG= LoggerFactory.getLogger(SerializationFactory.class.getName());
    private List<Serialization<?>> serializations=new ArrayList<>();

    public SerializationFactory(Configuration conf){
        super(conf);
        conf.getTrimmedStrings()
    }
}
