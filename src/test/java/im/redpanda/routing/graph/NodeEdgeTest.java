package im.redpanda.routing.graph;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class NodeEdgeTest {

  @Test
  void comparator() {
    ArrayList<NodeEdge> nodeEdges = new ArrayList<>();
    NodeEdge oldEdge = new NodeEdge();
    oldEdge.lastTimeCheckStarted = 5;
    NodeEdge newEdge = new NodeEdge();
    newEdge.lastTimeCheckStarted = 50;
    nodeEdges.add(oldEdge);
    nodeEdges.add(newEdge);

    Collections.sort(nodeEdges);
    assertEquals(oldEdge, nodeEdges.getFirst());
  }

  @Test
  void comparatorMaxValue() {
    ArrayList<NodeEdge> nodeEdges = new ArrayList<>();
    NodeEdge oldEdge = new NodeEdge();
    oldEdge.lastTimeCheckStarted = 5;
    NodeEdge newEdge = new NodeEdge();
    newEdge.lastTimeCheckStarted = System.currentTimeMillis();
    nodeEdges.add(oldEdge);
    nodeEdges.add(newEdge);

    Collections.sort(nodeEdges);
    assertEquals(oldEdge, nodeEdges.getFirst());
  }

  @Test
  void comparatorMinValue() {
    ArrayList<NodeEdge> nodeEdges = new ArrayList<>();
    NodeEdge oldEdge = new NodeEdge();
    oldEdge.lastTimeCheckStarted = 5;
    NodeEdge newEdge = new NodeEdge();
    newEdge.lastTimeCheckStarted = System.currentTimeMillis();

    nodeEdges.add(newEdge);
    nodeEdges.add(oldEdge);

    Collections.sort(nodeEdges);
    assertEquals(oldEdge, nodeEdges.getFirst());
  }
}
