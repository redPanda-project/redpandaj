package im.redpanda.routing.graph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import im.redpanda.core.KademliaId;
import im.redpanda.core.NodeId;
import im.redpanda.core.ServerContext;
import java.security.Security;
import org.junit.jupiter.api.Test;

class NodeTest {

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  @Test
  void getByKademliaId() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    NodeStore nodeStore = serverContext.getNodeStore();

    int size = nodeStore.size();
    System.out.println("Size of NodeStore: " + size);

    Node node = new Node(serverContext, new NodeId());

    KademliaId kademliaId = node.getNodeId().getKademliaId();

    Node byKademliaId = Node.getByKademliaId(serverContext, kademliaId);

    assertTrue(byKademliaId != null);

    assertEquals(byKademliaId.getNodeId().getKademliaId(), kademliaId);

    assertTrue(byKademliaId.getNodeId().hasPrivate());

    assertEquals(1, nodeStore.size() - size);
  }
}
