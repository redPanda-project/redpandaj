package im.redpanda.core;

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

        // Build a simple DER-like signature: 0x30, len, followed by padding bytes
        int sigLen = 72;
        byte[] sig = new byte[sigLen];
        sig[0] = 0x30;
        sig[1] = (byte) (sigLen - 2);
        for (int i = 2; i < sigLen; i++) sig[i] = (byte) i;

        int payloadLen = 4 + 8 + NodeId.PUBLIC_KEYLEN + 4 + content.length + sig.length;

        ByteBuffer buf = ByteBuffer.allocate(payloadLen);
        buf.putInt(ackId);
        buf.putLong(timestamp);
        buf.put(pub);
        buf.putInt(content.length);
        buf.put(content);
        buf.put(sig);
        buf.flip();

        int consumed = proc.parseCommand(Command.KADEMLIA_GET_ANSWER, buf, peer);

        assertEquals(1 + payloadLen, consumed);
    }
}

