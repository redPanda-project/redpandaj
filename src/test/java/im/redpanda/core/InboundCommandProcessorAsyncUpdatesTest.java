package im.redpanda.core;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class InboundCommandProcessorAsyncUpdatesTest {

    private ServerContext ctx;
    private InboundCommandProcessor proc;

    @Before
    public void setup() {
        ctx = ServerContext.buildDefaultServerContext();
        proc = new InboundCommandProcessor(ctx);
        ByteBufferPool.init();
        // Ensure Settings treat this as non-seed with updates disabled by default
        Settings.seedNode = false;
        Settings.loadUpdates = false;
    }

    @After
    public void cleanup() {
        // Remove files created by tests
        new File("redpanda.jar").delete();
        new File("tmp_redpanda.jar").delete();
        new File(ConnectionReaderThread.ANDROID_UPDATE_FILE).delete();
    }

    private static byte[] derSignature(int totalLen) {
        byte[] sig = new byte[totalLen];
        sig[0] = 0x30; // sequence
        sig[1] = (byte) (totalLen - 2);
        for (int i = 2; i < totalLen; i++) sig[i] = (byte) i;
        return sig;
    }

    @Test
    public void updateRequestContent_sendsJarFrame_whenSignaturePresent() throws Exception {
        // Prepare a small jar file at default path
        byte[] data = "jar".getBytes();
        try (FileOutputStream fos = new FileOutputStream("redpanda.jar")) {
            fos.write(data);
        }

        // Set signature and timestamp to pass initial guards
        ctx.getLocalSettings().setUpdateTimestamp(System.currentTimeMillis());
        ctx.getLocalSettings().setUpdateSignature(derSignature(72));

        Peer peer = new Peer("127.0.0.1", 5555, ctx.getNodeId());
        peer.setConnected(false); // avoid SelectionKey usage in tests
        ctx.getPeerList().add(peer);
        // Big enough buffer; code may grow it, but start with capacity
        peer.writeBuffer = ByteBuffer.allocate(1024);

        int consumed = proc.parseCommand(Command.UPDATE_REQUEST_CONTENT, ByteBuffer.allocate(0), peer);
        assertEquals(1, consumed);

        // Runnable sleeps 200ms before writing; wait a bit and assert buffer filled
        Thread.sleep(400);
        assertTrue(peer.writeBuffer.position() > 0);
        assertEquals(Command.UPDATE_ANSWER_CONTENT, peer.writeBuffer.get(0));
    }

    @Test
    public void androidUpdateRequestContent_doesNotSend_whenSignatureInvalid() throws IOException, InterruptedException {
        // Prepare android.apk data
        byte[] data = "apk".getBytes();
        try (FileOutputStream fos = new FileOutputStream(ConnectionReaderThread.ANDROID_UPDATE_FILE)) {
            fos.write(data);
        }

        // Set android timestamp and an invalid signature to trigger verify(false) and early return
        ctx.getLocalSettings().setUpdateAndroidTimestamp(System.currentTimeMillis());
        ctx.getLocalSettings().setUpdateAndroidSignature(derSignature(72));

        Peer peer = new Peer("127.0.0.1", 6666, ctx.getNodeId());
        peer.setConnected(false);
        ctx.getPeerList().add(peer);
        peer.writeBuffer = ByteBuffer.allocate(2048);

        int consumed = proc.parseCommand(Command.ANDROID_UPDATE_REQUEST_CONTENT, ByteBuffer.allocate(0), peer);
        assertEquals(1, consumed);

        // Wait briefly; verify no bytes were written due to failed verification
        Thread.sleep(200);
        assertEquals(0, peer.writeBuffer.position());
    }

    @Test
    public void updateAnswerContent_writesTempJar_whenNewer() throws Exception {
        Peer peer = new Peer("127.0.0.1", 7777, ctx.getNodeId());
        peer.setConnected(true);
        ctx.getPeerList().add(peer);

        long newerTs = ctx.getLocalSettings().getUpdateTimestamp() + 1000;
        byte[] data = new byte[]{9,8,7,6};
        byte[] sig = derSignature(72);

        ByteBuffer in = ByteBuffer.allocate(8 + 4 + sig.length + data.length);
        in.putLong(newerTs);
        in.putInt(data.length);
        in.put(sig);
        in.put(data);
        in.flip();

        int consumed = proc.parseCommand(Command.UPDATE_ANSWER_CONTENT, in, peer);
        assertEquals(1 + 8 + 4 + sig.length + data.length, consumed);

        File tmp = new File("tmp_redpanda.jar");
        assertTrue(tmp.exists());
        assertEquals(data.length, tmp.length());
    }
}

