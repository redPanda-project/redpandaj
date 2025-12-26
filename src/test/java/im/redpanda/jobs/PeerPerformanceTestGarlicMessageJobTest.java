package im.redpanda.jobs;

import im.redpanda.core.Node;
import im.redpanda.core.ServerContext;
import im.redpanda.flaschenpost.GMContent;
import im.redpanda.flaschenpost.GMParser;
import java.security.Security;
import java.util.ArrayList;
import org.junit.Test;

public class PeerPerformanceTestGarlicMessageJobTest {

  private static final ServerContext serverContext = ServerContext.buildDefaultServerContext();

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  @Test
  public void calculateNestedGarlicMessagesTest() {

    ArrayList<Node> nodes = new ArrayList<Node>();

    Node nodeA = new Node(serverContext, serverContext.getNodeId());
    nodes.add(nodeA);

    Node nodeB = new Node(serverContext, serverContext.getNodeId());
    nodes.add(nodeB);

    PeerPerformanceTestGarlicMessageJob peerPerformanceTestGarlicMessageJob =
        new PeerPerformanceTestGarlicMessageJob(serverContext);

    byte[] bytes = peerPerformanceTestGarlicMessageJob.calculateNestedGarlicMessages(nodes, 1);

    GMContent parse = GMParser.parse(serverContext, bytes);
    // todo assert?
  }
}
