package im.redpanda.routing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import im.redpanda.core.Command;
import im.redpanda.core.NodeId;
import im.redpanda.core.Peer;
import im.redpanda.core.PeerTestSupport;
import im.redpanda.core.ServerContext;
import im.redpanda.proto.FlaschenpostPut;
import org.junit.jupiter.api.Test;

class GMParserReproductionTest {

  private static class TestPeer extends Peer {

    TestPeer(String ip, int port, NodeId nodeId) {
      super(ip, port, nodeId);
    }

    @Override
    public boolean setWriteBufferFilled() {

      return true;
    }
  }

  private static byte[] garlicMessageBytes(ServerContext serverContext, NodeId target) {
    GarlicMessage garlicMessage = new GarlicMessage(serverContext, target);
    return garlicMessage.getContent();
  }

  @Test
  void sendFpToPeer_sendsValidProtobuf() throws Exception {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    NodeId target = NodeId.generateWithSimpleKey();
    byte[] content = garlicMessageBytes(serverContext, target);

    TestPeer peer = new TestPeer("10.0.0.1", 1000, target);
    PeerTestSupport.initWriteBuffer(peer, content.length + 1024);
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
    PeerTestSupport.writeBuffer(peer).flip();
    byte command = PeerTestSupport.writeBuffer(peer).get();
    assertEquals(Command.FLASCHENPOST_PUT, command);

    int length = PeerTestSupport.writeBuffer(peer).getInt();
    byte[] payload = new byte[length];
    PeerTestSupport.writeBuffer(peer).get(payload);

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
