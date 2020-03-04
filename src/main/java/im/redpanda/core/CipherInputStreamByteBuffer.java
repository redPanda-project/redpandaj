package im.redpanda.core;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class CipherInputStreamByteBuffer extends CipherInputStream {


    public CipherInputStreamByteBuffer(InputStream inputStream, Cipher cipher) {
        super(inputStream, cipher);
    }

    /**
     * Reads the bytes of this CipherStream into the ByteBuffer. ByteBuffer has to be in writing mode.
     *
     * @param byteBuffer
     * @return
     * @throws IOException
     */
    public void read(ByteBuffer byteBuffer) throws IOException {
        int read = read(byteBuffer.array(), byteBuffer.position(), byteBuffer.limit());
        byteBuffer.position(byteBuffer.position() + read);
    }
}
