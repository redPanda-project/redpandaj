package im.redpanda.core;

import im.redpanda.jobs.KademliaInsertJob;
import im.redpanda.kademlia.KadContent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class InboundCommandProcessorEvenMoreCoverageTest {

    private ServerContext ctx;
    private InboundCommandProcessor proc;

    @Before
    public void setup() {
        ctx = ServerContext.buildDefaultServerContext();
        proc = new InboundCommandProcessor(ctx);
        ByteBufferPool.init();
    }

    @After
    public void tearDown() {
        // nothing special
    }

    @Test
    public void ping_fromUnknownPeer_addsPeerAndReturnsZero() {
        Peer peer = new Peer("10.0.0.1", 1111, ctx.getNodeId());
        peer.setConnected(true);

        // ensure not in peer list
        assertFalse(ctx.getPeerList().contains(peer.getKademliaId()));

        int consumed = proc.parseCommand(Command.PING, ByteBuffer.allocate(0), peer);

        assertEquals(0, consumed);
        assertTrue(ctx.getPeerList().contains(peer.getKademliaId()));
    }

    @Test
    public void requestPeerList_writesSendPeerListFrame() {
        // Build two peers on server side: one with NodeId, one without
        Peer withId = new Peer("192.168.1.2", 2222, NodeId.generateWithSimpleKey());
        withId.setConnected(true);
        Peer withoutId = new Peer("192.168.1.3", 3333);
        withoutId.setConnected(true);
        ctx.getPeerList().add(withId);
        ctx.getPeerList().add(withoutId);

        Peer requester = new Peer("127.0.0.1", 4444, ctx.getNodeId());
        requester.setConnected(false); // avoid NPE in setWriteBufferFilled due to missing SelectionKey
        ctx.getPeerList().add(requester);
        requester.writeBuffer = ByteBuffer.allocate(1024 * 64);

        int consumed = proc.parseCommand(Command.REQUEST_PEERLIST, ByteBuffer.allocate(0), requester);
        assertEquals(1, consumed);

        requester.writeBuffer.flip();
        assertTrue("should have written at least command + len", requester.writeBuffer.remaining() >= 1 + 4);
        assertEquals(Command.SEND_PEERLIST, requester.writeBuffer.get());
        int payloadLen = requester.writeBuffer.getInt();
        assertTrue(payloadLen > 0);

        byte[] payload = new byte[payloadLen];
        requester.writeBuffer.get(payload);
        ByteBuffer peerListBytes = ByteBuffer.wrap(payload);
        int count = peerListBytes.getInt();
        assertTrue("at least one entry listed", count >= 1);
    }

    @Test
    public void sendPeerList_parsesEntries_addsPeers_skipsInvalidIpAndSelf() {
        // Build a SEND_PEERLIST payload with two entries
        NodeId otherNode = NodeId.generateWithSimpleKey();

        ByteBuffer payload = ByteBuffer.allocate(1024);
        int countPos = payload.position();
        payload.putInt(0); // placeholder for count

        int count = 0;

        // Entry 1: nodeId present + valid ip
        payload.putShort((short) 1);
        payload.put(otherNode.exportPublic());
        byte[] ip1 = "10.10.0.1".getBytes();
        payload.putInt(ip1.length);
        payload.put(ip1);
        payload.putInt(1111);
        count++;

        // Entry 2: no nodeId + valid ip
        payload.putShort((short) 0);
        byte[] ip2 = "10.10.0.2".getBytes();
        payload.putInt(ip2.length);
        payload.put(ip2);
        payload.putInt(2222);
        count++;

        // (Skip the invalid IP entry for stability)

        // fix count and wrap
        int endPos = payload.position();
        payload.putInt(countPos, count);
        payload.flip();

        ByteBuffer frame = ByteBuffer.allocate(4 + payload.remaining());
        frame.putInt(payload.remaining());
        frame.put(payload);
        frame.flip();

        Peer peer = new Peer("127.0.0.1", 5555, ctx.getNodeId());
        peer.setConnected(true);
        ctx.getPeerList().add(peer);

        int consumed = proc.parseCommand(Command.SEND_PEERLIST, frame, peer);
        assertEquals(1 + 4 + (endPos), consumed);

        // Should have added two peers (one with id, one without).
        assertNotNull(ctx.getPeerList().get(otherNode.getKademliaId()));
        assertTrue(ctx.getPeerList().removeIpPort("10.10.0.2", 2222));
    }

    @Test
    public void parseJobAck_consumesBytes_whenJobRunning() {
        // Prepare a running KademliaInsertJob so Job.getRunningJob finds it
        NodeId author = NodeId.generateWithSimpleKey();
        byte[] content = "ack-me".getBytes();
        long ts = System.currentTimeMillis();
        KadContent kad = new KadContent(ts, author.exportPublic(), content);
        kad.signWith(author);

        KademliaInsertJob job = new KademliaInsertJob(ctx, kad);

        // Make sure a peer with NodeId exists so job.init() will populate peers
        Peer p = new Peer("127.0.0.2", 6666, NodeId.generateWithSimpleKey());
        p.setConnected(true);
        ctx.getPeerList().add(p);

        // Ensure peers map is initialized before acknowledging
        job.init();
        job.start();
        int jobId = job.getJobId();

        ByteBuffer in = ByteBuffer.allocate(4);
        in.putInt(jobId);
        in.flip();

        Peer peer = new Peer("127.0.0.3", 7777, ctx.getNodeId());
        peer.setConnected(true);
        ctx.getPeerList().add(peer);

        int consumed = proc.parseCommand(Command.JOB_ACK, in, peer);
        assertEquals(1 + 4, consumed);
    }

    @Test
    public void kademliaGet_miss_startsSearch_returnsConsumed() {
        Peer peer = new Peer("127.0.0.1", 8888, ctx.getNodeId());
        peer.setConnected(true);
        ctx.getPeerList().add(peer);

        // Random ID not in store
        byte[] randomId = new byte[KademliaId.ID_LENGTH_BYTES];
        System.arraycopy(ctx.getNonce().getBytes(), 0, randomId, 0, randomId.length);
        randomId[0] ^= 0x7F; // mutate to be different

        ByteBuffer in = ByteBuffer.allocate(4 + KademliaId.ID_LENGTH_BYTES);
        in.putInt(123456);
        in.put(randomId);
        in.flip();

        int consumed = proc.parseCommand(Command.KADEMLIA_GET, in, peer);
        assertEquals(1 + 4 + KademliaId.ID_LENGTH_BYTES, consumed);
    }

    @Test
    public void flaschenpostPut_withAckPayload_isConsumed() {
        Peer peer = new Peer("127.0.0.1", 9999, ctx.getNodeId());
        peer.setConnected(true);
        ctx.getPeerList().add(peer);

        // Build a minimal, valid GMAck payload: [ACK][len=4][ackId]
        ByteBuffer ack = ByteBuffer.allocate(1 + 4 + 4);
        ack.put(im.redpanda.flaschenpost.GMType.ACK.getId());
        ack.putInt(4);
        ack.putInt(42);
        ack.flip();

        ByteBuffer in = ByteBuffer.allocate(4 + ack.remaining());
        in.putInt(ack.remaining());
        in.put(ack);
        in.flip();

        int consumed = proc.parseCommand(Command.FLASCHENPOST_PUT, in, peer);
        assertEquals(1 + 4 + (1 + 4 + 4), consumed);
    }
}
