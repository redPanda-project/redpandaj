package im.redpanda.core;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.jetbrains.annotations.NotNull;

public class PeerOutputStream extends OutputStream {

  private ByteBuffer byteBuffer;

  public void setByteBuffer(ByteBuffer byteBuffer) {
    this.byteBuffer = byteBuffer;
  }

  @Override
  public void write(int b) throws IOException {
    if (byteBuffer == null) {
      throw new IOException("No target ByteBuffer set");
    }
    byteBuffer.put((byte) b);
  }

  @Override
  public void write(@NotNull byte[] b) throws IOException {
    if (byteBuffer == null) {
      throw new IOException("No target ByteBuffer set");
    }
    byteBuffer.put(b);
  }

  @Override
  public void write(@NotNull byte[] b, int off, int len) throws IOException {
    if (byteBuffer == null) {
      throw new IOException("No target ByteBuffer set");
    }
    byteBuffer.put(b, off, len);
  }
}
