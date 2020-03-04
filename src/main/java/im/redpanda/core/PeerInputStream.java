package im.redpanda.core;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class PeerInputStream extends InputStream {

    private ByteBuffer byteBuffer;

    /**
     * byteBuffer has to be in read mode.
     * @param byteBuffer
     */
    public void setByteBuffer(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    @Override
    public int read() throws IOException {
        throw new RuntimeException("jzrtuzrtuzrtuz");
    }

    @Override
    public int read(@NotNull byte[] b) throws IOException {

//        byteBuffer.flip();
        int toReadBytes = Math.min(b.length, byteBuffer.remaining());
        byteBuffer.get(b, 0, toReadBytes);
//        byteBuffer.compact();

        return toReadBytes;
    }

    @Override
    public int read(@NotNull byte[] b, int off, int len) throws IOException {
        throw new RuntimeException("c4u5i6vg546vgz546gu546");
    }
}
