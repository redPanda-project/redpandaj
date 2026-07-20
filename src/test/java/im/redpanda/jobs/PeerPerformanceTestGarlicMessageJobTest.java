package im.redpanda.jobs;

import static org.junit.Assert.assertTrue;

import im.redpanda.core.Node;
import im.redpanda.core.NodeId;
import im.redpanda.core.ServerContext;
import im.redpanda.flaschenpost.GMParser;
import im.redpanda.store.NodeEdge;
import java.lang.reflect.Method;
import java.security.Security;
import java.util.ArrayList;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;
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

    GMParser.parse(serverContext, bytes);
    // todo assert?
  }

  /**
   * Regression for REDPANDAJ-2DW/2E5-class races: {@code dismissCheckByTimeoutIfEdgeQualityBad}
   * used to call {@code getEdgeWeight(edge)} without first checking that the edge is still in the
   * graph. If another thread (e.g. {@code NodeStore.maintainNodes}) removed the edge between the
   * caller copying the edge list and this check running, the intrusive edge specifics lookup throws
   * instead of the edge simply being skipped. Verifies the edge is dismissed cleanly instead of
   * throwing once it's no longer in the graph.
   */
  @Test
  public void dismissCheckByTimeoutIfEdgeQualityBad_edgeRemovedFromGraphIsDismissedNotThrown()
      throws Exception {
    Node nodeA = new Node(serverContext, serverContext.getNodeId());
    Node nodeB = new Node(serverContext, NodeId.generateWithSimpleKey());

    DefaultDirectedWeightedGraph<Node, NodeEdge> graph =
        new DefaultDirectedWeightedGraph<>(NodeEdge.class);
    graph.addVertex(nodeA);
    graph.addVertex(nodeB);
    NodeEdge edge = graph.addEdge(nodeA, nodeB);
    graph.setEdgeWeight(edge, 100.0);
    edge.setLastCheckFailed(true);

    // Simulate a concurrent maintainNodes() removing the edge after the caller snapshotted it.
    graph.removeEdge(edge);

    PeerPerformanceTestGarlicMessageJob job =
        new PeerPerformanceTestGarlicMessageJob(serverContext);
    Method method =
        PeerPerformanceTestGarlicMessageJob.class.getDeclaredMethod(
            "dismissCheckByTimeoutIfEdgeQualityBad",
            DefaultDirectedWeightedGraph.class,
            NodeEdge.class);
    method.setAccessible(true);

    boolean dismissed = (boolean) method.invoke(job, graph, edge);

    assertTrue(dismissed);
  }
}
