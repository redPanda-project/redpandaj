package im.redpanda.jobs;

import im.redpanda.core.*;
import im.redpanda.flaschenpost.GMAck;
import im.redpanda.flaschenpost.GarlicMessage;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleWeightedGraph;

import java.util.ArrayList;
import java.util.Collections;

public class PeerPerformanceTestGarlicMessageJob extends Job {

    ArrayList<Node> nodes;
    boolean success = false;
    private Peer flaschenPostInsertPeer;

    public PeerPerformanceTestGarlicMessageJob() {
        super(2500L);
        nodes = new ArrayList<>();
    }

    public static byte[] calculateNestedGarlicMessages(ArrayList<Node> nodes, int jobId) {
        //lets target to ourselves without the private key!
        NodeId targetId = NodeId.importPublic(Server.nodeId.exportPublic());

        GMAck gmAck = new GMAck(jobId);

        GarlicMessage currentLayer = new GarlicMessage(targetId);
        currentLayer.addGMContent(gmAck);


        for (Node node : nodes) {

            GarlicMessage newLayer = new GarlicMessage(node.getNodeId());
            newLayer.addGMContent(currentLayer);

            currentLayer = newLayer;

        }


        byte[] content = currentLayer.getContent();
        return content;
    }

    @Override
    public void init() {

        Server.nodeStore.maintainNodes();

        flaschenPostInsertPeer = PeerList.getGoodPeer();

        if (!flaschenPostInsertPeer.isConnected() || flaschenPostInsertPeer.getNode() == null) {
            return;
        }

        flaschenPostInsertPeer.getNode().cleanChecks();

        int garlicSequenceLenght = 2;

        SimpleWeightedGraph<Node, DefaultEdge> g = Server.nodeStore.getNodeGraph();
        if (g.vertexSet().size() < garlicSequenceLenght) {
            return;
        }


        ArrayList<Node> values = new ArrayList<>(g.vertexSet());

        Collections.shuffle(values);

        this.nodes = new ArrayList<>();
        Node currentNode = values.get(0);
        nodes.add(currentNode);
        int currentLength = 0;

        while (currentLength < garlicSequenceLenght) {
            ArrayList<DefaultEdge> edges = new ArrayList<>(g.outgoingEdgesOf(currentNode));
            Collections.shuffle(edges);
            for (DefaultEdge e : edges) {
                // if a edge is bad we should only test it rarely
                if (g.getEdgeWeight(e) < -0.8) {
                    if (Math.random() < 0.95f)
                        continue;
                } else if (g.getEdgeWeight(e) < 0) {
                    if (Math.random() < 0.8f)
                        continue;
                } else if (g.getEdgeWeight(e) < 0.8) {
                    if (Math.random() < 0.6f)
                        continue;
                }


                Node target = g.getEdgeSource(e);
                if (target == currentNode) {
                    target = g.getEdgeTarget(e);
                }

                if (nodes.contains(target)) {
                    continue;
                }

                currentNode = target;
                nodes.add(currentNode);
                currentNode.cleanChecks();
                break;
            }

            currentLength++;


        }

        String a = "";
        Node nodeBefore = null;
        for (Node node : nodes) {
            if (nodeBefore != null) {
                a += " -(" + String.format("%.1f", g.getEdgeWeight(g.getEdge(nodeBefore, node))) + ")-> " + node;
            } else {
                a += node;
            }
            nodeBefore = node;
        }
        System.out.println("path: " + a);


        byte[] content = calculateNestedGarlicMessages(this.nodes, getJobId());

        flaschenPostInsertPeer.getWriteBufferLock().lock();
        try {
            flaschenPostInsertPeer.getWriteBuffer().put(Command.FLASCHENPOST_PUT);
            flaschenPostInsertPeer.getWriteBuffer().putInt(content.length);
            flaschenPostInsertPeer.getWriteBuffer().put(content);
            flaschenPostInsertPeer.setWriteBufferFilled();
        } finally {
            flaschenPostInsertPeer.getWriteBufferLock().unlock();
        }


    }

    @Override
    public void work() {
        if (getEstimatedRuntime() > 1000L * 4L) {
            System.out.println("garlic check max timeout reached... " + getEstimatedRuntime());

            if (flaschenPostInsertPeer != null && flaschenPostInsertPeer.getNode() != null)
                done();
        }
    }

    @Override
    public void done() {
        super.done();

        float scoreToAdd = 0;
        if (success) {
            flaschenPostInsertPeer.getNode().increaseGmTestsSuccessful();

            for (Node node : nodes) {
                node.increaseGmTestsSuccessful();
                node.seen();
            }

            scoreToAdd = 0.1f;
        } else {
            flaschenPostInsertPeer.getNode().increaseGmTestsFailed();
            for (Node node : nodes) {
                node.increaseGmTestsFailed();
            }
            scoreToAdd = -0.1f;
        }

        SimpleWeightedGraph<Node, DefaultEdge> g = Server.nodeStore.getNodeGraph();

        String a = "";
        Node nodeBefore = null;
        for (Node node : nodes) {
            if (nodeBefore != null) {
                DefaultEdge edge = g.getEdge(nodeBefore, node);
                double newWeight = g.getEdgeWeight(edge) + scoreToAdd;
                if (newWeight > 1) {
                    newWeight = 1;
                } else if (newWeight < -1) {
                    newWeight = -1;
                }
                g.setEdgeWeight(edge, newWeight);
                a += " -(" + String.format("%.1f", newWeight) + ")-> " + node;
            } else {
                a += node;
            }
            nodeBefore = node;
        }
        System.out.println("path: " + a + " (updated)");

    }

    public void success() {
        success = true;
        done();
    }
}
