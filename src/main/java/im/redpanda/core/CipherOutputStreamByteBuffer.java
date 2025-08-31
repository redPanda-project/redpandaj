package im.redpanda.core;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class CipherOutputStreamByteBuffer extends CipherOutputStream {
    public CipherOutputStreamByteBuffer(OutputStream outputStream, Cipher cipher) {
        super(outputStream, cipher);
    }


    @Override
    public void write(byte[] bytes) throws IOException {
        super.write(bytes);
    }

    @Override
    public void write(int i) throws IOException {
        super.write(i);
    }

    @Override
    public void write(byte[] bytes, int i, int i1) throws IOException {
        super.write(bytes, i, i1);
    }

    /**
     * ByteBuffer has to be in reading mode before calling this method.
     *
     * @param byteBuffer
     * @throws IOException
     */
    public void write(ByteBuffer byteBuffer) throws IOException {
        int len = byteBuffer.remaining();
        if (len <= 0) {
            return;
        }
        int pos = byteBuffer.position();
        write(byteBuffer.array(), pos, len);
        byteBuffer.position(pos + len);
    }

    /**
     * ByteBuffer has to be in reading mode before calling this method.
     *
     * @param byteBuffer
     * @throws IOException
     */
    public void write(ByteBuffer byteBuffer, int lenToWrite) throws IOException {
        int len = Math.min(lenToWrite, byteBuffer.remaining());
        if (len <= 0) {
            return;
        }
        int pos = byteBuffer.position();
        write(byteBuffer.array(), pos, len);
        byteBuffer.position(pos + len);
    }
}
