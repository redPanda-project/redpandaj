package im.redpanda.store;

import static org.junit.Assert.*;

import im.redpanda.core.Node;
import im.redpanda.core.NodeId;
import im.redpanda.core.ServerContext;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;
import org.junit.Test;

public class NodeStoreMaintainNodesTest {

  @Test
  public void maintain_addsVerticesAndEdges_andRemovesBadNodes() {
    ServerContext ctx = ServerContext.buildDefaultServerContext();

    // Create and register the server node
    Node serverNode = new Node(ctx, ctx.getNodeId());
    ctx.setNode(serverNode);

    NodeStore store = ctx.getNodeStore();
    store.clearGraph();

    // Add several candidate nodes into the store (higher score preferred)
    Node[] nodes = new Node[6];
    for (int i = 0; i < nodes.length; i++) {
      nodes[i] = new Node(ctx, new NodeId());
      // Make first few look good; last one will be bad-scored later
      nodes[i].setGmTestsSuccessful(10 - i);
    }

    // Run maintenance a few rounds to grow the graph
    for (int r = 0; r < 10; r++) {
      store.maintainNodes();
    }

    DefaultDirectedWeightedGraph<Node, NodeEdge> g = store.getNodeGraph();
    assertTrue(g.vertexSet().size() >= 2);

    // Ensure random edge addition kicks in
    boolean hasSomeEdge = !g.edgeSet().isEmpty();
    assertTrue(hasSomeEdge);

    // Now mark the last node as very bad and ensure it gets removed
    Node bad = nodes[nodes.length - 1];
    bad.setGmTestsFailed(50); // strong negative score

    // Run maintenance again; removal requires at least 4 nodes present
    for (int r = 0; r < 10; r++) {
      store.maintainNodes();
    }

    // Bad node should either be absent from graph or blacklisted
    boolean present = g.containsVertex(bad);
    assertFalse("bad node should be removed from graph", present);
    assertTrue("bad node should be blacklisted", bad.isBlacklisted());
  }
}
