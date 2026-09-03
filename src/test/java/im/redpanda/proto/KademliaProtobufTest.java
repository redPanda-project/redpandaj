package im.redpanda.proto;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.protobuf.ByteString;
import im.redpanda.core.*;
import im.redpanda.core.PeerTestSupport;
import im.redpanda.dht.KadContent;
import im.redpanda.identity.KademliaId;
import im.redpanda.identity.NodeId;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class KademliaProtobufTest {

  @Test
  void kademliaStoreSerialization() throws Exception {
    NodeId nodeId = new NodeId();
    byte[] content = "test-content".getBytes();
    KadContent kadContent =
        new KadContent(System.currentTimeMillis(), nodeId.exportPublic(), content);
    kadContent.signWith(nodeId);

    KademliaStore storeMsg =
        KademliaStore.newBuilder()
            .setJobId(123)
            .setTimestamp(kadContent.getTimestamp())
            .setPublicKey(ByteString.copyFrom(kadContent.getPubkey()))
            .setContent(ByteString.copyFrom(kadContent.getContent()))
            .setSignature(ByteString.copyFrom(kadContent.getSignature()))
            .build();

    byte[] serialized = storeMsg.toByteArray();
    KademliaStore parsed = KademliaStore.parseFrom(serialized);

    assertEquals(123, parsed.getJobId());
    assertEquals(kadContent.getTimestamp(), parsed.getTimestamp());
    assertArrayEquals(kadContent.getPubkey(), parsed.getPublicKey().toByteArray());
    assertArrayEquals(kadContent.getContent(), parsed.getContent().toByteArray());
    assertArrayEquals(kadContent.getSignature(), parsed.getSignature().toByteArray());
  }

  @Test
  void kademliaGetSerialization() throws Exception {
    KademliaId id = new KademliaId();
    KademliaGet getMsg =
        KademliaGet.newBuilder()
            .setJobId(456)
            .setSearchedId(
                KademliaIdProto.newBuilder()
                    .setKeyBytes(ByteString.copyFrom(id.getBytes()))
                    .build())
            .build();

    byte[] serialized = getMsg.toByteArray();
    KademliaGet parsed = KademliaGet.parseFrom(serialized);

    assertEquals(456, parsed.getJobId());
    assertArrayEquals(id.getBytes(), parsed.getSearchedId().getKeyBytes().toByteArray());
  }

  @Test
  void inboundKademliaStore() throws Exception {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    InboundCommandProcessor processor = new InboundCommandProcessor(serverContext);

    NodeId nodeId = new NodeId();
    byte[] content = "data".getBytes();
    KadContent kadContent =
        new KadContent(System.currentTimeMillis(), nodeId.exportPublic(), content);
    kadContent.signWith(nodeId);

    KademliaStore storeMsg =
        KademliaStore.newBuilder()
            .setJobId(789)
            .setTimestamp(kadContent.getTimestamp())
            .setPublicKey(ByteString.copyFrom(kadContent.getPubkey()))
            .setContent(ByteString.copyFrom(kadContent.getContent()))
            .setSignature(ByteString.copyFrom(kadContent.getSignature()))
            .build();

    byte[] payload = storeMsg.toByteArray();
    ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + payload.length);
    buffer.put(Command.KADEMLIA_STORE);
    buffer.putInt(payload.length);
    buffer.put(payload);
    buffer.flip();

    Peer peer = new Peer("1.1.1.1", 1234);
    peer.setConnected(true);
    PeerTestSupport.initWriteBuffer(peer, 1024);
    peer.setSelectionKey(new TestSelectionKey());

    int read = processor.parseCommand(buffer.get(), buffer, peer);
    assertEquals(1 + 4 + payload.length, read);

    // Verify stored in KadStoreManager
    KadContent stored = serverContext.getKadStoreManager().get(kadContent.getId());
    assertNotNull(stored);
    assertArrayEquals(content, stored.getContent());
  }

  // Minimal SelectionKey mock for testing
  private static class TestSelectionKey extends java.nio.channels.SelectionKey {
    private int ops = 0;

    @Override
    public java.nio.channels.SelectableChannel channel() {
      return null;
    }

    @Override
    public java.nio.channels.Selector selector() {
      return new java.nio.channels.Selector() {
        @Override
        public boolean isOpen() {
          return true;
        }

        @Override
        public java.nio.channels.spi.SelectorProvider provider() {
          return null;
        }

        @Override
        public java.util.Set<java.nio.channels.SelectionKey> keys() {
          return null;
        }

        @Override
        public java.util.Set<java.nio.channels.SelectionKey> selectedKeys() {
          return null;
        }

        @Override
        public int selectNow() {
          return 0;
        }

        @Override
        public int select(long timeout) {
          return 0;
        }

        @Override
        public int select() {
          return 0;
        }

        @Override
        public java.nio.channels.Selector wakeup() {
          return this;
        }

        @Override
        public void close() {}
      };
    }

    @Override
    public boolean isValid() {
      return true;
    }

    @Override
    public void cancel() {}

    @Override
    public int interestOps() {
      return ops;
    }

    @Override
    public java.nio.channels.SelectionKey interestOps(int ops) {
      this.ops = ops;
      return this;
    }

    @Override
    public int readyOps() {
      return 0;
    }
  }
}
