package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;

@InterfaceAudience.Public
@InterfaceStability.Unstable
/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/23
 **/
public class DataOutputOutputStream extends OutputStream {
    private final DataOutput out;
    public static OutputStream constructOutputStream(DataOutput out){
        if (out instanceof OutputStream) {
            return (OutputStream) out;
        }else {
            return new DataOutputOutputStream(out);
        }
    }
    private DataOutputOutputStream(DataOutput out){
        this.out=out;
    }

    @Override
    public void write(int b) throws IOException {
        out.writeByte(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        out.write(b,off,len);
    }

    @Override
    public void write(byte[] b) throws IOException {
        out.write(b);
    }
}
