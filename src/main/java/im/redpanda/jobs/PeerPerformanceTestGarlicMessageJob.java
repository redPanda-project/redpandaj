package im.redpanda.jobs;

import im.redpanda.core.*;
import im.redpanda.flaschenpost.GMAck;
import im.redpanda.flaschenpost.GarlicMessage;
import im.redpanda.store.NodeEdge;
import org.jgrapht.graph.SimpleWeightedGraph;

import java.util.ArrayList;
import java.util.Collections;

public class PeerPerformanceTestGarlicMessageJob extends Job {

    public static final double LINK_FAILED = 2;
    public static final double CUT_HARD = 3;
    public static final int CUT_MID = 5;
    public static final double CUT_LOW = 8;
    public static final int MAX_WEIGHT = 10;
    public static final int MIN_WEIGHT = 1;
    public static final float DELTA_SUCESS = 1;
    public static final float DELTA_FAIL = -1;
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

        flaschenPostInsertPeer = PeerList.getGoodPeer();

        if (!flaschenPostInsertPeer.isConnected() || flaschenPostInsertPeer.getNode() == null) {
            return;
        }

        flaschenPostInsertPeer.getNode().cleanChecks();

        int garlicSequenceLenght = 4;

        SimpleWeightedGraph<Node, NodeEdge> g = Server.nodeStore.getNodeGraph();
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
            ArrayList<NodeEdge> edges = new ArrayList<>(g.outgoingEdgesOf(currentNode));
            Collections.shuffle(edges);
            for (NodeEdge e : edges) {
                // if a edge is bad we should only test it rarely

                if (e.isLastCheckFailed()) {
                    if (g.getEdgeWeight(e) < CUT_HARD) {
                        if (Math.random() < 0.98f)
                            continue;
                    } else if (g.getEdgeWeight(e) < CUT_MID) {
                        if (Math.random() < 0.9f)
                            continue;
                    } else if (g.getEdgeWeight(e) < CUT_LOW) {
                        if (Math.random() < 0.7f)
                            continue;
                    }
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

        if (nodes.size() < 2) {
            super.done();
            return;
        }

//        String a = "";
//        Node nodeBefore = null;
//        for (Node node : nodes) {
//            if (nodeBefore != null) {
//                a += " -(" + String.format("%.1f", g.getEdgeWeight(g.getEdge(nodeBefore, node))) + ")-> " + node;
//            } else {
//                a += node;
//            }
//            nodeBefore = node;
//        }
//        System.out.println("path: " + a);


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
//            System.out.println("garlic check failed " + getEstimatedRuntime() + ", path: " +
//                    printPath());


            if (flaschenPostInsertPeer != null && flaschenPostInsertPeer.getNode() != null)
                done();
        }
    }

    private String printPath() {
        SimpleWeightedGraph<Node, NodeEdge> g = Server.nodeStore.getNodeGraph();

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
        return a;
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

            scoreToAdd = DELTA_SUCESS;
        } else {
            flaschenPostInsertPeer.getNode().increaseGmTestsFailed();
            for (Node node : nodes) {
                node.increaseGmTestsFailed();
            }
            scoreToAdd = DELTA_FAIL;
        }

        SimpleWeightedGraph<Node, NodeEdge> g = Server.nodeStore.getNodeGraph();

        String a = "";
        Node nodeBefore = null;
        for (Node node : nodes) {
            if (nodeBefore != null) {
                NodeEdge edge = g.getEdge(nodeBefore, node);
                double newWeight = g.getEdgeWeight(edge) + scoreToAdd;
                if (newWeight > MAX_WEIGHT) {
                    newWeight = MAX_WEIGHT;
                } else if (newWeight < MIN_WEIGHT) {
                    newWeight = MIN_WEIGHT;
                }
                g.setEdgeWeight(edge, newWeight);
                edge.setLastCheckFailed(!success);
                a += " -(" + String.format("%.1f", newWeight) + ")-> " + node;
            } else {
                a += node;
            }
            nodeBefore = node;
        }
        System.out.println("path: " + a + " (updated " + (success ? "success" : "failed") + ")");

    }

    public void success() {
        success = true;
        done();
    }
}
