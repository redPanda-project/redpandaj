package im.redpanda.store;

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
}
