package im.redpanda.jobs;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import im.redpanda.core.Node;
import im.redpanda.core.NodeId;
import im.redpanda.core.Peer;
import im.redpanda.core.ServerContext;
import im.redpanda.flaschenpost.GMParser;
import im.redpanda.store.NodeEdge;
import java.lang.reflect.Field;
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

  /**
   * Regression for REDPANDAJ-2EG: {@code done()} used to dereference {@code
   * flaschenPostInsertPeer.getNode()}. init() validates the node under the NodeStore write lock,
   * but the GMAck answering the test message arrives much later ({@code GMParser} -> {@code
   * success()} -> {@code done()}); if the insert peer disconnected in the meantime, {@code
   * Peer.disconnect()} has already called {@code clearNode()} and {@code getNode()} returns null,
   * so the success path threw an NPE. done() must instead use the node reference captured during
   * init() and complete without exception.
   */
  @Test
  public void done_insertPeerDisconnectedBetweenInitAndAck_successPathDoesNotThrow()
      throws Exception {
    PeerPerformanceTestGarlicMessageJob job = buildJobWithDisconnectedInsertPeer();

    // GMAck arrives after the peer disconnected: must terminate cleanly, not NPE.
    job.success();
  }

  /**
   * Same race as above but for the failure path (timeout via {@code work()} or the init()
   * disconnect guard calling {@code done()} directly): line "increaseGmTestsFailed" used to NPE the
   * same way, which also defeated the init() guard that relies on done() for a clean abort.
   */
  @Test
  public void done_insertPeerDisconnectedBetweenInitAndTimeout_failPathDoesNotThrow()
      throws Exception {
    PeerPerformanceTestGarlicMessageJob job = buildJobWithDisconnectedInsertPeer();

    job.done();
  }

  /**
   * Builds a job in the state left behind by a successful init() — nodes path computed, insert peer
   * and its node captured — after which the insert peer disconnects ({@code clearNode()}), as seen
   * in REDPANDAJ-2EG.
   */
  private PeerPerformanceTestGarlicMessageJob buildJobWithDisconnectedInsertPeer()
      throws Exception {
    Node ownNode = new Node(serverContext, serverContext.getNodeId());
    Node insertNode = new Node(serverContext, NodeId.generateWithSimpleKey());

    PeerPerformanceTestGarlicMessageJob job =
        new PeerPerformanceTestGarlicMessageJob(serverContext);
    job.nodes = new ArrayList<>();
    job.nodes.add(ownNode);
    job.nodes.add(insertNode);
    job.nodes.add(ownNode);

    Peer insertPeer = new Peer("127.0.0.1", 1234, insertNode.getNodeId());
    insertPeer.setNode(insertNode);

    setPrivateField(job, "flaschenPostInsertPeer", insertPeer);
    // init() captures the validated node reference exactly once, under the NodeStore write lock.
    setPrivateField(job, "insertNode", insertNode);

    // Peer disconnects before the GMAck/timeout triggers done(): Peer.disconnect() clears the
    // node as its very first step, after which getNode() returns null.
    insertPeer.clearNode();
    assertNull(insertPeer.getNode());

    return job;
  }

  private static void setPrivateField(Object target, String name, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }
}
