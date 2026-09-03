package im.redpanda.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import im.redpanda.core.Node;
import im.redpanda.core.NodeId;
import im.redpanda.core.ServerContext;
import java.security.Security;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class NodeStoreTest {

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  @Test
  void blacklist() {
    Map<Node, Long> nodeBlacklist = new HashMap<>();

    Node node = new Node(ServerContext.buildDefaultServerContext(), new NodeId());

    nodeBlacklist.put(node, System.currentTimeMillis());

    assertTrue(nodeBlacklist.containsKey(node));

    assertTrue(System.currentTimeMillis() - nodeBlacklist.get(node) < 1000L * 60L * 5L);
  }

  /**
   * T117 gave all three cache tiers explicit serializers. The on-heap tier must still hand back the
   * very object that was put in: {@code Node.seen(...)}, {@code touchBlacklisted()} and the score
   * counters mutate a node in place and expect the store to see it. A serializing on-heap tier
   * would return copies and silently drop every one of those mutations.
   */
  @Test
  void onHeapTierReturnsTheStoredInstanceNotACopy() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    Node node = new Node(serverContext, new NodeId());

    Node fetched = serverContext.getNodeStore().get(node.getNodeId().getKademliaId());

    assertThat(fetched).isSameAs(node);

    fetched.setGmTestsSuccessful(9);
    assertThat(
            serverContext
                .getNodeStore()
                .get(node.getNodeId().getKademliaId())
                .getGmTestsSuccessful())
        .isEqualTo(9);
  }
}
