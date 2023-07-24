package org.apache.hadoop.io;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.io.*;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/23
 **/
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class CompressedWritable implements Writable {
    private byte[] compressed;
    public CompressedWritable(){}

    @Override
    public void readFields(DataInput in) throws IOException {
        compressed=new byte[in.readInt()];
        in.readFully(compressed,0,compressed.length);
    }
    protected void ensureInflated(){
        if (compressed != null) {
            try {
                ByteArrayInputStream deflated = new ByteArrayInputStream(compressed);
                DataInput inflater = new DataInputStream(new InflaterInputStream(deflated));
                readFieldsCompressed(inflater);
                compressed=null;
            }catch (IOException e){
                throw new RuntimeException(e);
            }
        }
    }
    protected abstract void readFieldsCompressed(DataInput in)throws IOException;
    protected abstract void writeCompressed(DataOutput out)throws IOException;

    @Override
    public void writer(DataOutput out) throws IOException {
        if (compressed == null) {
            ByteArrayOutputStream deflated = new ByteArrayOutputStream();
            Deflater deflater = new Deflater(Deflater.BEST_SPEED);
            DataOutputStream dout = new DataOutputStream(new DeflaterOutputStream(deflated, deflater));
            writeCompressed(dout);
            dout.close();
            deflater.end();
            compressed=deflated.toByteArray();
        }
        out.writeInt(compressed.length);
        out.write(compressed);
    }
}
