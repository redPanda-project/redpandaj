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
        write(byteBuffer.array(), byteBuffer.position(), byteBuffer.limit());
        byteBuffer.position(byteBuffer.limit());
    }
}
