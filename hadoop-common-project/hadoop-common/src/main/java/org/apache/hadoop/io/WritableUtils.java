package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.DataInput;
import java.io.DataOutput;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class WritableUtils {

    public static int readVInt(DataInput in) {
        return 0;
    }

    public static String[] readCompressedStringArray(DataInput in) {
    }

    public static void writeVInt(DataOutput out, int size) {

    }

    public static void writeCompressedStringArray(DataOutput out, String[] strings) {
    }
}
