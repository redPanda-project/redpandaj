package im.redpanda.store;

import static org.junit.Assert.assertTrue;

import im.redpanda.core.Node;
import im.redpanda.core.NodeId;
import im.redpanda.core.ServerContext;
import java.security.Security;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class NodeStoreTest {

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  @Test
  public void testBlacklist() {
    Map<Node, Long> nodeBlacklist = new HashMap<>();

    Node node = new Node(ServerContext.buildDefaultServerContext(), new NodeId());

    nodeBlacklist.put(node, System.currentTimeMillis());

    assertTrue(nodeBlacklist.containsKey(node));

    assertTrue(System.currentTimeMillis() - nodeBlacklist.get(node) < 1000L * 60L * 5L);
  }
}
