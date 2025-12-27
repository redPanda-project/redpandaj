package im.redpanda.core;

import static com.google.protobuf.ByteString.copyFrom;
import static org.junit.Assert.*;

import com.google.protobuf.InvalidProtocolBufferException;
import im.redpanda.kademlia.KadContent;
import im.redpanda.proto.JobAck;
import im.redpanda.proto.KademliaGetAnswer;
import im.redpanda.proto.KademliaStore;
import java.nio.ByteBuffer;
import org.junit.Test;

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
    for (int i = 2; i < sigLen; i++) sig[i] = (byte) i;

    KademliaGetAnswer answerMsg =
        KademliaGetAnswer.newBuilder()
            .setAckId(ackId)
            .setTimestamp(timestamp)
            .setPublicKey(copyFrom(pub))
            .setContent(copyFrom(content))
            .setSignature(copyFrom(sig))
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

    KademliaStore storeMsg =
        KademliaStore.newBuilder()
            .setJobId(jobId)
            .setTimestamp(kadContent.getTimestamp())
            .setPublicKey(copyFrom(kadContent.getPubkey()))
            .setContent(copyFrom(kadContent.getContent()))
            .setSignature(copyFrom(kadContent.getSignature()))
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
      JobAck ackProto = JobAck.parseFrom(ackBytes);
      org.junit.Assert.assertEquals(jobId, ackProto.getJobId());
    } catch (InvalidProtocolBufferException e) {
      org.junit.Assert.fail("Failed to parse ACK protobuf: " + e.getMessage());
    }
  }

  @Test
  public void parseCommand_requestPeerList_skipsPeerWithNullIp()
      throws InvalidProtocolBufferException {
    ServerContext ctx = ServerContext.buildDefaultServerContext();
    InboundCommandProcessor proc = newProcessor(ctx);

    // Add valid peer
    Peer validPeer = new Peer("127.0.0.1", 11111, ctx.getNodeId());
    ctx.getPeerList().add(validPeer);

    // Add invalid peer (null IP)
    Peer invalidPeer = new Peer("1.2.3.4", 22222);
    ctx.getPeerList().add(invalidPeer);
    invalidPeer.ip = null;

    Peer requestingPeer = new Peer("10.0.0.1", 33333, ctx.getNodeId());
    requestingPeer.setConnected(true);
    requestingPeer.writeBuffer = ByteBuffer.allocate(4096);

    ByteBuffer dummy = ByteBuffer.allocate(0);
    // This call would throw NPE before the fix
    int consumed = proc.parseCommand(Command.REQUEST_PEERLIST, dummy, requestingPeer);

    assertEquals(1, consumed);

    // Check response
    requestingPeer.writeBuffer.flip();
    assertEquals(Command.SEND_PEERLIST, requestingPeer.writeBuffer.get());
    int len = requestingPeer.writeBuffer.getInt();
    byte[] data = new byte[len];
    requestingPeer.writeBuffer.get(data);

    im.redpanda.proto.SendPeerList sendPeerList = im.redpanda.proto.SendPeerList.parseFrom(data);

    boolean foundInvalid = false;
    for (im.redpanda.proto.PeerInfoProto p : sendPeerList.getPeersList()) {
      if (p.getPort() == 22222) foundInvalid = true;
    }
    assertFalse("Peer with null IP should be skipped", foundInvalid);
    assertTrue(sendPeerList.getPeersCount() >= 1);
  }
}
