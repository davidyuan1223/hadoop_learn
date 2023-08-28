package org.apache.hadoop.crypto;

import org.apache.hadoop.crypto.CryptoCodec;
import org.apache.hadoop.crypto.CryptoStreamUtils;
import org.apache.hadoop.crypto.Decryptor;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class CryptoInputStream extends FilterInputStream implements
        Seekable, PositionedReadable, ByteBufferReadable, HasFileDescriptor,
        CanSetDropBehind,CanSetReadahead,HasEnhanceByteBufferAccess,
        ReadableByteChannel,CanUnbuffer,StreamCapabilities,
ByteBufferPositionedReadable, IOStatisticsSource {
    private final byte[] oneByteBuf=new byte[1];
    private final CryptoCodec codec;
    private final Decryptor decryptor;
    private final int bufferSize;
    private ByteBuffer inBuffer;
    private ByteBuffer outBuffer;
    private long streamOffset=0;
    private Boolean usingBytesBufferRead=null;
    private byte padding;
    private boolean closed;
    private final byte[] key;
    private final byte[] initIV;
    private byte[] iv;
    private final boolean isByteBufferReadable;
    private final boolean isReadableByteChannel;
    private final Queue<ByteBuffer> bufferPool=new ConcurrentLinkedQueue<>();
    private final Queue<Decryptor> decryptorPool=new ConcurrentLinkedQueue<>();
    public CryptoInputStream(InputStream in,CryptoCodec codec,int bufferSize,byte[] key,byte[] iv)throws IOException{
        this(in,codec,bufferSize,key,iv, CryptoStreamUtils.getInputStreamOffset(in));
    }
    public CryptoInputStream(InputStream in,CryptoCodec codec,int bufferSize,byte[] key,byte[] iv,long streamOffset)throws IOException{
        super(in);
        CryptoStreamUtils.checkCodec(codec);
        this.bufferSize=CryptoStreamUtils.checkBufferSize(codec,bufferSize);
        this.codec=codec;
        this.key=key.clone();
        this.initIV=iv.clone();
        this.iv=iv.clone();
        this.streamOffset=streamOffset;
        isByteBufferReadable=in instanceof ByteBufferReadable;
        isReadableByteChannel=in instanceof ReadableByteChannel;
        inBuffer=ByteBuffer.allocateDirect(this.bufferSize);
        outBuffer=ByteBuffer.allocateDirect(this.bufferSize);
        decryptor=getDecryptor();
        resetStreamOffset(streamOffset);
    }
    public CryptoInputStream(InputStream in,CryptoCodec codec,byte[] key,byte[] iv)throws IOException{
        this(in,codec,CryptoStreamUtils.getBufferSize(codec.getConf()),key,iv);
    }

    public InputStream getWrappedStream(){
        return in;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        checkStream();
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }
        final int remaining = outBuffer.remaining();
        if (remaining>0) {
            int n = Math.min(len, remaining);
            outBuffer.get(b,off,n);
            return n;
        }else {
            int n=0;
            if (usingBytesBufferRead == null) {
                if (isByteBufferReadable || isReadableByteChannel) {
                    try {
                        n=isByteBufferReadable?((ByteBufferReadable)in).read(inBuffer):
                                ((ReadableByteChannel)in).read(inBuffer);
                        usingBytesBufferRead=Boolean.TRUE;
                    }catch (UnsupportedOperationException e){
                        usingBytesBufferRead=Boolean.FALSE;
                    }
                }else {
                    usingBytesBufferRead=Boolean.FALSE;
                }
                if (!usingBytesBufferRead) {
                    n=readFromUnderlyingStream(inBuffer);
                }
            }else {
                if (usingBytesBufferRead) {
                    n=isReadableByteChannel?
                            ((ByteBufferReadable)in).read(inBuffer):
                            ((ReadableByteChannel)in).read(inBuffer);
                }else {
                    n=readFromUnderlyingStream(inBuffer);
                }
            }
            if (n <= 0) {
                return n;
            }
            streamOffset+=n;
            decrypt(decryptor,inBuffer,outBuffer,padding);
            padding=afterDecryption(decryptor,inBuffer,streamOffset,iv);
            n=Math.min(len,outBuffer.remaining());
            outBuffer.get(b,off,n);
            return n;
        }
    }
    private int readFromUnderlyingStream(ByteBuffer inBuffer)throws IOException{
        final int toRead=inBuffer.remaining();
        final byte[] tmp=getTmpBuf();
        final int n=in.read(tmp,0,toRead);
        if (n > 0) {
            inBuffer.put(tmp,0,n);
        }
        return n;
    }
    private byte[] tmpBuf;
    private byte[] getTmpBuf(){
        if (tmpBuf == null) {
            tmpBuf=new byte[bufferSize];
        }
        return tmpBuf;
    }
    private void decrypt(Decryptor decryptor,ByteBuffer inBuffer,ByteBuffer outBuffer,byte padding)throws IOException{
        Preconditions.checkState(inBuffer.position()>=padding);
        if (inBuffer.position() == padding) {
            return;
        }
        inBuffer.flip();
        outBuffer.clear();
        decryptor.decrypt(inBuffer,outBuffer);
        inBuffer.clear();
        outBuffer.flip();
        if (padding > 0) {
            outBuffer.position(padding);
        }
    }
    private byte afterDecryption(Decryptor decryptor,ByteBuffer inBuffer,long position,byte[] iv)throws IOException{
        byte padding=0;
        if (decryptor.isContextReset()) {
            updateDecryptor(decryptor,position,iv);
            padding=getPadding(position);
            inBuffer.position(padding);
        }
        return padding;
    }
    private long getCounter(long position){
        return position/codec.getCipherSuite().getAlgoBlockSize();
    }
    private byte getPadding(long position){
        return (byte) (position%codec.getCipherSuite().getAlgoBlockSize());
    }
    private void updateDecryptor(Decryptor decryptor,long position,byte[] iv)throws IOException{
        final long counter=getCounter(position);
        codec.calculateIV(initIV,counter,iv);
        decryptor.init(key,iv);
    }
    private void resetStreamOffset(long offset)throws IOException{
        streamOffset=offset;
        inBuffer.clear();
        outBuffer.clear();
        outBuffer.limit(0);
        updateDecryptor(decryptor,offset,iv);
        padding=getPadding(offset);
        inBuffer.position(padding);
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        super.close();
        freeBuffers();
        codec.close();
        closed=true;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
        checkStream();
        if (!(in instanceof PositionedReadable)) {
            throw new UnsupportedOperationException(in.getClass().getCanonicalName()+" does not support positioned read.");
        }
        final int n=((PositionedReadable)in).read(position,buffer,offset,length);
        if (n>0) {
            decrypt(position,buffer,offset,n);
        }
        return n;
    }

    @Override
    public int read(long position, ByteBuffer buf) throws IOException {
        checkStream();
        if (!(in instanceof ByteBufferPositionedReadable)) {
            throw new UnsupportedOperationException(in.getClass().getCanonicalName()+" does not support positioned reads with byte buffers.");
        }
        int bufPos = buf.position();
        final int n=((ByteBufferPositionedReadable)in).read(position,buf);
        if (n > 0) {
            decrypt(position,buf,n,bufPos);
        }
        return n;
    }

    @Override
    public void readFully(long position, ByteBuffer buf) throws IOException {
        checksStream();
        if (!(in instanceof ByteBufferPositionedReadable)) {
            throw new UnsupportedOperationException(in.getClass().getCanonicalName()+" does not support positioned reads with byte buffers.");
        }
        int bufPos = buf.position();
        ((ByteBufferPositionedReadable)in).readFully(position,buf);
        final int n=buf.position()-bufPos;
        if (n>0){
            decrypt(position,buf,n,bufPos);
        }
    }
    private void decrypt(long position,byte[] buffer,int offset,int length)throws IOException{
        ByteBuffer localInBuffer=null;
        ByteBuffer localOutBuffer=null;
        Decryptor decryptor=null;
        try {
            localInBuffer=getBuffer();
            localOutBuffer=getBuffer();
            decryptor=getDecryptor();
            byte[] iv = initIV.clone();
            updateDecryptor(decryptor,position,iv);
            byte padding=getPadding(position);
            localInBuffer.position(padding);
            int n=0;
            while (n < length) {
                int toDecrypt = Math.min(length - n, localInBuffer.remaining());
                localInBuffer.put(buffer,offset+n,toDecrypt);
                decrypt(decryptor,localInBuffer,localOutBuffer,padding);
                localOutBuffer.get(buffer,offset+n,toDecrypt);
                n+=toDecrypt;
                padding=afterDecryption(decryptor,localInBuffer,position+n,iv);
            }
        }finally {
            returnBuffer(localInBuffer);
            returnBuffer(localOutBuffer);
            returnDecryptor(decryptor);
        }
    }
    private void decrypt(long filePosition,ByteBuffer buf,int length,int start)throws IOException{
        ByteBuffer localInBuffer=null;
        ByteBuffer localOutBuffer=null;
        buf=buf.duplicate();
        int decryptedBytes=0;
        Decryptor localDecryptor=null;
        try {
            localInBuffer=getBuffer();
            localOutBuffer=getBuffer();
            localDecryptor=getDecryptor();
        }
    }
}

