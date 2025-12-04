package im.redpanda.core;

import im.redpanda.kademlia.KadContent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class InboundCommandProcessorMoreCoverageTest {

    private ServerContext ctx;
    private InboundCommandProcessor proc;

    @Before
    public void setup() {
        ctx = ServerContext.buildDefaultServerContext();
        proc = new InboundCommandProcessor(ctx);
    }

    @After
    public void cleanup() {
        // Ensure android.apk from tests is removed
        File f = new File(ConnectionReaderThread.ANDROID_UPDATE_FILE);
        if (f.exists()) {
            // ignore deletion result
            f.delete();
        }
        // Remove tmp_redpanda.jar if created
        File f2 = new File("tmp_redpanda.jar");
        if (f2.exists()) {
            f2.delete();
        }
    }

    @Test
    public void kademliaGet_servesFromStore() {
        Peer peer = new Peer("127.0.0.1", 12345, ctx.getNodeId());
        peer.setConnected(true);
        ctx.getPeerList().add(peer);
        peer.writeBuffer = ByteBuffer.allocate(4096);

        // Create content and put to store
        NodeId author = NodeId.generateWithSimpleKey();
        byte[] content = "hello-store".getBytes();
        long ts = System.currentTimeMillis();
        KadContent stored = new KadContent(ts, author.exportPublic(), content);
        stored.signWith(author);
        ctx.getKadStoreManager().put(stored);

        int jobId = 101;
        ByteBuffer in = ByteBuffer.allocate(4 + KademliaId.ID_LENGTH_BYTES);
        in.putInt(jobId);
        in.put(stored.getId().getBytes());
        in.flip();

        int consumed = proc.parseCommand(Command.KADEMLIA_GET, in, peer);
        assertEquals(1 + 4 + KademliaId.ID_LENGTH_BYTES, consumed);

        peer.writeBuffer.flip();
        assertEquals(Command.KADEMLIA_GET_ANSWER, peer.writeBuffer.get());
        assertEquals(jobId, peer.writeBuffer.getInt());
        long tsWritten = peer.writeBuffer.getLong();
        assertEquals(ts, tsWritten);
        byte[] pub = new byte[NodeId.PUBLIC_KEYLEN];
        peer.writeBuffer.get(pub);
        assertArrayEquals(author.exportPublic(), pub);
        int len = peer.writeBuffer.getInt();
        assertEquals(content.length, len);
    }

    @Test
    public void updateAnswerTimestamp_consumesBytes() {
        Peer peer = new Peer("127.0.0.1", 12345, ctx.getNodeId());
        peer.setConnected(true);
        ctx.getPeerList().add(peer);

        long ts = ctx.getLocalSettings().getUpdateTimestamp();
        ByteBuffer in = ByteBuffer.allocate(8);
        in.putLong(ts);
        in.flip();

        int consumed = proc.parseCommand(Command.UPDATE_ANSWER_TIMESTAMP, in, peer);
        assertEquals(1 + 8, consumed);
        assertFalse(in.hasRemaining());
    }

    @Test
    public void androidUpdateRequestTimestamp_behavesDependingOnFilePresence() throws IOException {
        Peer peer = new Peer("127.0.0.1", 12345, ctx.getNodeId());
        // Avoid NPE in setWriteBufferFilled by marking as not connected (still writes to buffer)
        peer.setConnected(false);
        ctx.getPeerList().add(peer);
        peer.writeBuffer = ByteBuffer.allocate(32);

        // Ensure apk does not exist
        File f = new File(ConnectionReaderThread.ANDROID_UPDATE_FILE);
        if (f.exists()) f.delete();

        int consumedNoFile = proc.parseCommand(Command.ANDROID_UPDATE_REQUEST_TIMESTAMP, ByteBuffer.allocate(0), peer);
        assertEquals(1, consumedNoFile);

        // Create an empty android.apk so branch writes answer
        assertTrue(f.createNewFile());
        long before = ctx.getLocalSettings().getUpdateAndroidTimestamp();
        int consumed = proc.parseCommand(Command.ANDROID_UPDATE_REQUEST_TIMESTAMP, ByteBuffer.allocate(0), peer);
        assertEquals(1, consumed);

        peer.writeBuffer.flip();
        assertEquals(Command.ANDROID_UPDATE_ANSWER_TIMESTAMP, peer.writeBuffer.get());
        long writtenTs = peer.writeBuffer.getLong();
        assertEquals(before, writtenTs);
    }

    private static byte[] derSignature(int totalLen) {
        // Build a minimal DER-like signature with declared length totalLen
        byte[] sig = new byte[totalLen];
        sig[0] = 0x30; // SEQUENCE
        sig[1] = (byte) (totalLen - 2); // remaining length
        for (int i = 2; i < totalLen; i++) sig[i] = (byte) i;
        return sig;
    }

    @Test
    public void androidUpdateAnswerContent_updatesFileAndSettings() {
        Peer peer = new Peer("127.0.0.1", 12345, ctx.getNodeId());
        peer.setConnected(true);
        ctx.getPeerList().add(peer);

        long newerTs = ctx.getLocalSettings().getUpdateAndroidTimestamp() + 1000;
        byte[] data = "apk".getBytes();
        byte[] sig = derSignature(72);

        ByteBuffer in = ByteBuffer.allocate(8 + 4 + sig.length + data.length);
        in.putLong(newerTs);
        in.putInt(data.length);
        in.put(sig);
        in.put(data);
        in.flip();

        int consumed = proc.parseCommand(Command.ANDROID_UPDATE_ANSWER_CONTENT, in, peer);
        assertEquals(1 + 8 + 4 + sig.length + data.length, consumed);

        // File should exist and settings updated
        File apk = new File(ConnectionReaderThread.ANDROID_UPDATE_FILE);
        assertTrue(apk.exists());
        assertEquals(newerTs, ctx.getLocalSettings().getUpdateAndroidTimestamp());
        assertArrayEquals(sig, ctx.getLocalSettings().getUpdateAndroidSignature());
    }

    @Test
    public void updateAnswerContent_consumesWithoutWritingWhenNotNewer() {
        Peer peer = new Peer("127.0.0.1", 12345, ctx.getNodeId());
        peer.setConnected(true);
        ctx.getPeerList().add(peer);

        long notNewerTs = ctx.getLocalSettings().getUpdateTimestamp();
        byte[] data = new byte[]{1,2,3,4,5};
        byte[] sig = derSignature(70);

        ByteBuffer in = ByteBuffer.allocate(8 + 4 + sig.length + data.length);
        in.putLong(notNewerTs);
        in.putInt(data.length);
        in.put(sig);
        in.put(data);
        in.flip();

        int consumed = proc.parseCommand(Command.UPDATE_ANSWER_CONTENT, in, peer);
        assertEquals(1 + 8 + 4 + sig.length + data.length, consumed);
        // tmp_redpanda.jar should not be created when not newer
        assertFalse(new File("tmp_redpanda.jar").exists());
    }
}
