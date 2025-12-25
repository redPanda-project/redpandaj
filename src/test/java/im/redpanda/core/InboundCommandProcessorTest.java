package im.redpanda.core;

import im.redpanda.kademlia.KadContent;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class InboundCommandProcessorTest {

    private InboundCommandProcessor newProcessor(ServerContext ctx) {
        return new InboundCommandProcessor(ctx);
    }

    @Test
    public void parseCommand_ping_writesPong() {
        ServerContext ctx = ServerContext.buildDefaultServerContext();
        InboundCommandProcessor proc = newProcessor(ctx);

        Peer peer = new Peer("127.0.0.1", 12345, ctx.getNodeId());
        peer.setConnected(true);
        // Ensure peer is recognized in peer list to satisfy PING precondition
        ctx.getPeerList().add(peer);

        // Prepare write buffer
        peer.writeBuffer = ByteBuffer.allocate(16);

        ByteBuffer dummy = ByteBuffer.allocate(0);
        int consumed = proc.parseCommand(Command.PING, dummy, peer);

        assertEquals(1, consumed);
        assertEquals(1, peer.writeBuffer.position());
        assertEquals(Command.PONG, peer.writeBuffer.get(0));
    }

    @Test
    public void parseCommand_pong_updatesMetrics() {
        ServerContext ctx = ServerContext.buildDefaultServerContext();
        InboundCommandProcessor proc = newProcessor(ctx);

        Peer peer = new Peer("127.0.0.1", 12345, ctx.getNodeId());
        peer.setConnected(true);
        ctx.getPeerList().add(peer);

        peer.lastPinged = System.currentTimeMillis() - 50;

        ByteBuffer dummy = ByteBuffer.allocate(0);
        int consumed = proc.parseCommand(Command.PONG, dummy, peer);

        assertEquals(1, consumed);
        assertTrue(peer.getLastPongReceived() > 0);
        assertTrue(peer.ping >= 0);
    }

    @Test
    public void parseCommand_kademliaGetAnswer_consumesExpectedLength() {
        ServerContext ctx = ServerContext.buildDefaultServerContext();
        InboundCommandProcessor proc = newProcessor(ctx);

        Peer peer = new Peer("127.0.0.1", 12345, ctx.getNodeId());
        peer.setConnected(true);
        ctx.getPeerList().add(peer);

        int ackId = 42;
        long timestamp = System.currentTimeMillis();
        byte[] pub = ctx.getNodeId().exportPublic();
        byte[] content = "hello".getBytes();

        // Build a simple DER-like signature
        int sigLen = 72;
        byte[] sig = new byte[sigLen];
        sig[0] = 0x30;
        sig[1] = (byte) (sigLen - 2);
        for (int i = 2; i < sigLen; i++)
            sig[i] = (byte) i;

        im.redpanda.proto.KademliaGetAnswer answerMsg = im.redpanda.proto.KademliaGetAnswer.newBuilder()
                .setAckId(ackId)
                .setTimestamp(timestamp)
                .setPublicKey(com.google.protobuf.ByteString.copyFrom(pub))
                .setContent(com.google.protobuf.ByteString.copyFrom(content))
                .setSignature(com.google.protobuf.ByteString.copyFrom(sig))
                .build();

        byte[] payload = answerMsg.toByteArray();

        ByteBuffer buf = ByteBuffer.allocate(4 + payload.length);
        buf.putInt(payload.length);
        buf.put(payload);
        buf.flip();

        int consumed = proc.parseCommand(Command.KADEMLIA_GET_ANSWER, buf, peer);

        assertEquals(1 + 4 + payload.length, consumed);
    }

    @Test
    public void parseCommand_kademliaStore_savesAndAcks() {
        ServerContext ctx = ServerContext.buildDefaultServerContext();
        InboundCommandProcessor proc = newProcessor(ctx);

        Peer peer = new Peer("127.0.0.1", 23456, ctx.getNodeId());
        peer.setConnected(true);
        ctx.getPeerList().add(peer);

        // Prepare write buffer for ACK
        peer.writeBuffer = ByteBuffer.allocate(256);

        // Build a valid KadContent signed by an arbitrary author key
        NodeId author = NodeId.generateWithSimpleKey();
        long timestamp = System.currentTimeMillis();
        byte[] pub = author.exportPublic();
        byte[] content = "store-me".getBytes();

        KadContent kadContent = new KadContent(timestamp, pub, content);
        kadContent.signWith(author);

        int jobId = 77;

        im.redpanda.proto.KademliaStore storeMsg = im.redpanda.proto.KademliaStore.newBuilder()
                .setJobId(jobId)
                .setTimestamp(kadContent.getTimestamp())
                .setPublicKey(com.google.protobuf.ByteString.copyFrom(kadContent.getPubkey()))
                .setContent(com.google.protobuf.ByteString.copyFrom(kadContent.getContent()))
                .setSignature(com.google.protobuf.ByteString.copyFrom(kadContent.getSignature()))
                .build();

        byte[] payload = storeMsg.toByteArray();
        ByteBuffer buf = ByteBuffer.allocate(4 + payload.length);
        buf.putInt(payload.length);
        buf.put(payload);
        buf.flip();

        // Exercise
        int consumed = proc.parseCommand(Command.KADEMLIA_STORE, buf, peer);

        // Verify consumed bytes
        org.junit.Assert.assertEquals(1 + 4 + payload.length, consumed);

        // Verify content is stored
        KadContent stored = ctx.getKadStoreManager().get(kadContent.getId());
        org.junit.Assert.assertNotNull(stored);

        // Verify ACK written
        peer.writeBuffer.flip();
        org.junit.Assert.assertTrue(peer.writeBuffer.remaining() >= 1 + 4);
        org.junit.Assert.assertEquals(Command.JOB_ACK, peer.writeBuffer.get());

        int ackLen = peer.writeBuffer.getInt();
        byte[] ackBytes = new byte[ackLen];
        peer.writeBuffer.get(ackBytes);

        try {
            im.redpanda.proto.JobAck ackProto = im.redpanda.proto.JobAck.parseFrom(ackBytes);
            org.junit.Assert.assertEquals(jobId, ackProto.getJobId());
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            org.junit.Assert.fail("Failed to parse ACK protobuf: " + e.getMessage());
        }
    }
}
