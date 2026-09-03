package im.redpanda.routing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import im.redpanda.core.NodeId;
import im.redpanda.core.ServerContext;
import java.security.Security;
import org.junit.jupiter.api.Test;

class FlaschenpostTest {

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  @Test
  void simpleTargetTest() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();

    NodeId targetId = NodeId.importPublic(serverContext.getNodeId().exportPublic());

    GarlicMessage garlicMessage = new GarlicMessage(serverContext, targetId);

    assertEquals(targetId.getKademliaId(), garlicMessage.destination);
  }
}
