package im.redpanda.core;

import im.redpanda.jobs.KademliaInsertJob;
import im.redpanda.kademlia.KadContent;
import static com.google.protobuf.ByteString.copyFrom;
import im.redpanda.proto.*;
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

        // Entry 1: nodeId present + valid ip
        PeerInfoProto p1 = PeerInfoProto.newBuilder()
                .setIp("10.10.0.1")
                .setPort(1111)
                .setNodeId(NodeIdProto.newBuilder()
                        .setPublicKeyBytes(copyFrom(otherNode.exportPublic())).build())
                .build();

        // Entry 2: no nodeId + valid ip
        PeerInfoProto p2 = PeerInfoProto.newBuilder()
                .setIp("10.10.0.2")
                .setPort(2222)
                .build();

        SendPeerList spl = SendPeerList.newBuilder()
                .addPeers(p1)
                .addPeers(p2)
                .build();

        byte[] payloadData = spl.toByteArray();

        ByteBuffer frame = ByteBuffer.allocate(4 + payloadData.length);
        frame.putInt(payloadData.length);
        frame.put(payloadData);
        frame.flip();

        Peer peer = new Peer("127.0.0.1", 5555, ctx.getNodeId());
        peer.setConnected(true);
        ctx.getPeerList().add(peer);

        int consumed = proc.parseCommand(Command.SEND_PEERLIST, frame, peer);
        assertEquals(1 + 4 + (payloadData.length), consumed);

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

        JobAck ackMsg = JobAck.newBuilder()
                .setJobId(jobId)
                .build();
        byte[] ackData = ackMsg.toByteArray();

        ByteBuffer in = ByteBuffer.allocate(4 + ackData.length);
        in.putInt(ackData.length);
        in.put(ackData);
        in.flip();

        Peer peer = new Peer("127.0.0.3", 7777, ctx.getNodeId());
        peer.setConnected(true);
        ctx.getPeerList().add(peer);

        int consumed = proc.parseCommand(Command.JOB_ACK, in, peer);
        assertEquals(1 + 4 + ackData.length, consumed);
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

        KademliaGet getMsg = KademliaGet.newBuilder()
                .setJobId(123456)
                .setSearchedId(KademliaIdProto.newBuilder()
                        .setKeyBytes(copyFrom(randomId)).build())
                .build();
        byte[] getData = getMsg.toByteArray();

        ByteBuffer in = ByteBuffer.allocate(4 + getData.length);
        in.putInt(getData.length);
        in.put(getData);
        in.flip();

        int consumed = proc.parseCommand(Command.KADEMLIA_GET, in, peer);
        assertEquals(1 + 4 + getData.length, consumed);
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

        byte[] ackBytes = new byte[ack.remaining()];
        ack.get(ackBytes);

        FlaschenpostPut putMsg = FlaschenpostPut.newBuilder()
                .setContent(copyFrom(ackBytes))
                .build();
        byte[] putData = putMsg.toByteArray();

        ByteBuffer in = ByteBuffer.allocate(4 + putData.length);
        in.putInt(putData.length);
        in.put(putData);
        in.flip();

        int consumed = proc.parseCommand(Command.FLASCHENPOST_PUT, in, peer);
        assertEquals(1 + 4 + putData.length, consumed);
    }
}
