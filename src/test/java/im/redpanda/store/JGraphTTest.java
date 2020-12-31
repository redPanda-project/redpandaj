package im.redpanda.store;

import im.redpanda.core.KademliaId;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleWeightedGraph;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.function.Supplier;

import static org.junit.Assert.*;

public class JGraphTTest {

    @Test
    public void testBasicGraphBehavior() {

        SimpleWeightedGraph<KademliaId, DefaultEdge> g = new SimpleWeightedGraph<KademliaId, DefaultEdge>(DefaultEdge.class);

        Supplier<KademliaId> s = () -> new KademliaId();

        g.setVertexSupplier(s);

        for (int i = 0; i < 4; i++) {
            g.addVertex();
        }

        for (int i = 0; i < 7; i++) {
            addRandomEdge(g);
        }

        assertEquals(6, g.edgeSet().size());

        for (DefaultEdge defaultEdge : g.edgeSet()) {
            DefaultEdge edge = g.getEdge(g.getEdgeTarget(defaultEdge), g.getEdgeSource(defaultEdge));

            assertEquals(0d, g.getEdgeWeight(defaultEdge), 0d);
            assertEquals(defaultEdge, edge);
            assertEquals(g.getEdgeSource(defaultEdge), g.getEdgeSource(edge));
        }


    }

    private void addRandomEdge(SimpleWeightedGraph<KademliaId, DefaultEdge> g) {

        Set<KademliaId> kademliaIds = g.vertexSet();

        ArrayList<KademliaId> ids = new ArrayList<>(kademliaIds);


        boolean added = false;
        int count = 0;

        while (!added && count < 100) {

            Collections.shuffle(ids);
            KademliaId a = ids.get(0);
            ids.remove(a);
            Collections.shuffle(ids);
            KademliaId b = ids.get(0);
            ids.add(a);

            DefaultEdge defaultEdge = g.addEdge(a, b);

            if (defaultEdge != null) {
                g.setEdgeWeight(defaultEdge, 0);
                added = true;
            }

            count++;
        }


    }

}
