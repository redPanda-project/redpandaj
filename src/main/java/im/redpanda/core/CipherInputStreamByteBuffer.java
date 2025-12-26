package im.redpanda.core;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;

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
    int remaining = byteBuffer.remaining();
    if (remaining <= 0) {
      return;
    }
    int pos = byteBuffer.position();
    int read = read(byteBuffer.array(), pos, remaining);
    if (read > 0) {
      byteBuffer.position(pos + read);
    }
  }
}
