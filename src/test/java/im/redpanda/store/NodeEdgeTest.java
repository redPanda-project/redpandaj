package im.redpanda.store;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class NodeEdgeTest {

    @Test
    public void testComparator() {
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
    public void testComparatorMaxValue() {
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
    public void testComparatorMinValue() {
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