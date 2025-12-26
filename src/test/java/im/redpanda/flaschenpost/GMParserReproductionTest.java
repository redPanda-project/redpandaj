package im.redpanda.flaschenpost;

import static org.junit.Assert.assertEquals;

import im.redpanda.core.Command;
import im.redpanda.core.NodeId;
import im.redpanda.core.Peer;
import im.redpanda.core.ServerContext;
import im.redpanda.proto.FlaschenpostPut;
import java.nio.ByteBuffer;
import org.junit.Test;

public class GMParserReproductionTest {

  private static class TestPeer extends Peer {
    boolean setWriteBufferCalled;

    TestPeer(String ip, int port, NodeId nodeId) {
      super(ip, port, nodeId);
    }

    @Override
    public boolean setWriteBufferFilled() {
      setWriteBufferCalled = true;
      return true;
    }
  }

  private static byte[] garlicMessageBytes(ServerContext serverContext, NodeId target) {
    GarlicMessage garlicMessage = new GarlicMessage(serverContext, target);
    return garlicMessage.getContent();
  }

  @Test
  public void sendFpToPeer_sendsValidProtobuf() throws Exception {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    NodeId target = NodeId.generateWithSimpleKey();
    byte[] content = garlicMessageBytes(serverContext, target);

    TestPeer peer = new TestPeer("10.0.0.1", 1000, target);
    peer.writeBuffer = ByteBuffer.allocate(content.length + 1024);
    peer.setConnected(true);
    serverContext.getPeerList().add(peer);

    // We wrap the content in a way that GMParser.parse will assume it is a
    // GarlicMessage type
    // The first byte of content is checked in GMParser.parse:
    // byte type = buffer.get();
    // if type == GMType.GARLIC_MESSAGE ...

    // Wait, GMParser.parse consumes the content to decide what to do.
    // If I pass my bad content, GMParser might reject it before sending.

    // Let's use a valid GarlicMessage content first.
    byte[] validContent = garlicMessageBytes(serverContext, target);

    GMParser.parse(serverContext, validContent);

    // Now check what was written to the peer
    peer.writeBuffer.flip();
    byte command = peer.writeBuffer.get();
    assertEquals(Command.FLASCHENPOST_PUT, command);

    int length = peer.writeBuffer.getInt();
    byte[] payload = new byte[length];
    peer.writeBuffer.get(payload);

    // This should parse successfully if the sender is correct.
    // Currently it sends raw bytes, which are NOT a valid FlaschenpostPut protobuf
    // message.
    // So this will throw InvalidProtocolBufferException.
    try {
      FlaschenpostPut.parseFrom(payload);
    } catch (Exception e) {
      System.out.println("Reproduction successful: " + e.getMessage());
      throw e;
    }
  }
}
