package im.redpanda.core;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class PeerOutputStream extends OutputStream {

    private ByteBuffer byteBuffer;


    public void setByteBuffer(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    @Override
    public void write(int b) throws IOException {
        throw new RuntimeException("dasdtruethret");
    }

    @Override
    public void write(@NotNull byte[] b) throws IOException {
        byteBuffer.put(b);
    }

    @Override
    public void write(@NotNull byte[] b, int off, int len) throws IOException {
        throw new RuntimeException("rztzthjzhdkjfgdjfkg");
    }
}
