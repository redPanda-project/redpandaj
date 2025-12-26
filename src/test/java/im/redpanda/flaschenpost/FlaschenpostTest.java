package im.redpanda.flaschenpost;

import static org.junit.Assert.assertEquals;

import im.redpanda.core.NodeId;
import im.redpanda.core.ServerContext;
import java.security.Security;
import org.junit.Test;

public class FlaschenpostTest {

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  @Test
  public void simpleTargetTest() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();

    NodeId targetId = NodeId.importPublic(serverContext.getNodeId().exportPublic());

    GMAck gmAck = new GMAck();

    GarlicMessage garlicMessage = new GarlicMessage(serverContext, targetId);

    assertEquals(targetId.getKademliaId(), garlicMessage.destination);
  }
}
