package im.redpanda.routing.graph;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.core.ServerContext;
import im.redpanda.identity.KademliaId;
import im.redpanda.identity.NodeId;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.Security;
import org.junit.jupiter.api.Test;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

/**
 * T117: the node cache must not go through MapDB's default (Elsa) serializer any more — it writes
 * fully qualified class names and needs {@code Serializable}, which is what made moving {@code
 * core/Node.java} a state-destroying change (DDD review §5).
 */
class NodeStoreSerializersTest {

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  private static <T> byte[] serialize(Serializer<T> serializer, T value) throws IOException {
    DataOutput2 out = new DataOutput2();
    serializer.serialize(out, value);
    return out.copyBytes();
  }

  private static <T> T deserialize(Serializer<T> serializer, byte[] bytes) throws IOException {
    return serializer.deserialize(new DataInput2.ByteArray(bytes), bytes.length);
  }

  @Test
  void kademliaIdRoundtrip() throws Exception {
    KademliaId id = new KademliaId();

    KademliaId loaded =
        deserialize(
            NodeStoreSerializers.KADEMLIA_ID, serialize(NodeStoreSerializers.KADEMLIA_ID, id));

    assertThat(loaded).isEqualTo(id);
    assertThat(loaded.getBytes()).isEqualTo(id.getBytes());
  }

  @Test
  void nodeRoundtripKeepsIdentityScoreAndConnectionPoints() throws Exception {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    Node node = new Node(serverContext, new NodeId());
    node.seen("203.0.113.7", 59558);
    node.setGmTestsSuccessful(4);
    node.setGmTestsFailed(2);

    Node loaded =
        deserialize(NodeStoreSerializers.NODE, serialize(NodeStoreSerializers.NODE, node));

    assertThat(loaded).isEqualTo(node);
    assertThat(loaded.getNodeId().getKademliaId()).isEqualTo(node.getNodeId().getKademliaId());
    assertThat(loaded.getGmTestsSuccessful()).isEqualTo(4);
    assertThat(loaded.getGmTestsFailed()).isEqualTo(2);
    assertThat(loaded.getLastSeen()).isEqualTo(node.getLastSeen());
    assertThat(loaded.latestSeenConnectionPoint().getIp()).isEqualTo("203.0.113.7");
    assertThat(loaded.latestSeenConnectionPoint().getPort()).isEqualTo(59558);
  }

  /** A blacklisted node has to stay blacklisted across a restart, or the ban is free to reset. */
  @Test
  void nodeRoundtripKeepsBlacklisting() throws Exception {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    Node node = new Node(serverContext, new NodeId());
    node.touchBlacklisted();

    Node loaded =
        deserialize(NodeStoreSerializers.NODE, serialize(NodeStoreSerializers.NODE, node));

    assertThat(loaded.isBlacklisted()).isTrue();
  }

  @Test
  void serializedNodePinsNoClassName() throws Exception {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    Node node = new Node(serverContext, new NodeId());

    String written = new String(serialize(NodeStoreSerializers.NODE, node), StandardCharsets.UTF_8);

    assertThat(written).contains("\"nodeId\"").doesNotContain("im.redpanda");
  }
}
