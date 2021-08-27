package im.redpanda.jobs;

import im.redpanda.core.*;
import im.redpanda.flaschenpost.GMAck;
import im.redpanda.flaschenpost.GarlicMessage;
import im.redpanda.store.NodeEdge;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;
import org.jgrapht.graph.SimpleWeightedGraph;

import java.util.ArrayList;
import java.util.Collections;

public class PeerPerformanceTestGarlicMessageJob extends Job {

    public static final int TEST_HOPS_MAX = 8;
    public static final int TEST_HOPS_MIN = 2;

    public static final double LINK_FAILED = 2;
    public static final double CUT_HARD = 3;
    public static final int CUT_MID = 5;
    public static final double CUT_LOW = 8;
    public static final int MAX_WEIGHT = 15;
    public static final int MIN_WEIGHT = 1;
    public static final float DELTA_SUCCESS = 1;
    public static final float DELTA_FAIL = -1;
    public static final long JOB_TIMEOUT = 1000L * 5L;

    private static int countSuccess = 0;
    private static int countFailed = 0;

    ArrayList<Node> nodes;
    boolean success = false;
    private Peer flaschenPostInsertPeer;

    public PeerPerformanceTestGarlicMessageJob(ServerContext serverContext) {
        super(serverContext, 2500L);
        nodes = new ArrayList<>();
    }

    public byte[] calculateNestedGarlicMessages(ArrayList<Node> nodes, int jobId) {
        //lets target to ourselves without the private key!
        NodeId targetId = NodeId.importPublic(serverContext.getNodeId().exportPublic());

        GMAck gmAck = new GMAck(jobId);

        GarlicMessage currentLayer = new GarlicMessage(serverContext, targetId);
        currentLayer.addGMContent(gmAck);


        for (Node node : nodes) {

            GarlicMessage newLayer = new GarlicMessage(serverContext, node.getNodeId());
            newLayer.addGMContent(currentLayer);

            currentLayer = newLayer;

        }


        byte[] content = currentLayer.getContent();
        return content;
    }

    @Override
    public void init() {
        DefaultDirectedWeightedGraph<Node, NodeEdge> nodeGraph = serverContext.getNodeStore().getNodeGraph();
        if (nodeGraph.vertexSet().isEmpty()) {
            super.done();
            return;
        }

        // nodes = hops + 1
        int garlicSequenceLength = TEST_HOPS_MIN + Server.random.nextInt(TEST_HOPS_MAX - TEST_HOPS_MIN) + 1;
        if (nodeGraph.vertexSet().size() < garlicSequenceLength) {
            garlicSequenceLength = nodeGraph.vertexSet().size();
        }


        ArrayList<Node> values = new ArrayList<>(nodeGraph.vertexSet());

        Collections.shuffle(values);
        Node startingNode = values.get(0);

        this.nodes = new ArrayList<>();
        Node currentNode = startingNode;
        nodes.add(currentNode);
        int currentLength = 0;

        while (currentLength < garlicSequenceLength) {
            ArrayList<NodeEdge> edges = new ArrayList<>(nodeGraph.outgoingEdgesOf(currentNode));
            Collections.shuffle(edges);
            for (NodeEdge edge : edges) {
                // if a edge is bad we should only test it rarely

                if (edge.isLastCheckFailed()) {
                    if (nodeGraph.getEdgeWeight(edge) < CUT_HARD) {
                        if (Math.random() < 0.98f)
                            continue;
                    } else if (nodeGraph.getEdgeWeight(edge) < CUT_MID) {
                        if (Math.random() < 0.9f)
                            continue;
                    } else if (nodeGraph.getEdgeWeight(edge) < CUT_LOW) {
                        if (Math.random() < 0.7f)
                            continue;
                    }
                }

                Node target = nodeGraph.getEdgeTarget(edge);

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

        byte[] content = calculateNestedGarlicMessages(this.nodes, getJobId());

        Peer closestGoodPeer = serverContext.getPeerList().getClosestGoodPeer(this.nodes.get(0).getNodeId().getKademliaId());
        if (closestGoodPeer == null) {
            super.done();
            return;
        }
        flaschenPostInsertPeer = closestGoodPeer;
        flaschenPostInsertPeer.getNode().cleanChecks();

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
        if (getEstimatedRuntime() > JOB_TIMEOUT) {
//            System.out.println("garlic check failed " + getEstimatedRuntime() + ", path: " +
//                    printPath());


            if (flaschenPostInsertPeer != null && flaschenPostInsertPeer.getNode() != null) {
                done();
            }
        }
    }

    private String printPath() {
        DefaultDirectedWeightedGraph<Node, NodeEdge> g = serverContext.getNodeStore().getNodeGraph();

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

        if (nodes.size() < 2) {
            throw new RuntimeException("job started with too less nodes, this should not happen");
        }

        float scoreToAdd = 0;
        if (success) {
            flaschenPostInsertPeer.getNode().increaseGmTestsSuccessful();

            for (Node node : nodes) {
                node.increaseGmTestsSuccessful();
                node.seen();
            }

            scoreToAdd = DELTA_SUCCESS;
        } else {
            flaschenPostInsertPeer.getNode().increaseGmTestsFailed();
            for (Node node : nodes) {
                node.increaseGmTestsFailed();
            }
            scoreToAdd = DELTA_FAIL;
        }

        DefaultDirectedWeightedGraph<Node, NodeEdge> nodeGraph = serverContext.getNodeStore().getNodeGraph();

        String pathString = "";
        Node nodeBefore = null;
        for (Node node : nodes) {
            if (nodeBefore != null) {
                NodeEdge edge = nodeGraph.getEdge(nodeBefore, node);
                if (!nodeGraph.containsEdge(edge)) {
                    continue;
                }
                double newWeight = nodeGraph.getEdgeWeight(edge) + scoreToAdd;
                if (newWeight > MAX_WEIGHT) {
                    newWeight = MAX_WEIGHT;
                } else if (newWeight < MIN_WEIGHT) {
                    newWeight = MIN_WEIGHT;
                }
                nodeGraph.setEdgeWeight(edge, newWeight);
                edge.setLastCheckFailed(!success);
                pathString += " -(" + String.format("%.1f", newWeight) + ")-> " + node;
            } else {
                pathString += node;
            }
            nodeBefore = node;
        }

        if (!success) {
            System.out.println("path: " + pathString + " hops: " + (nodes.size() - 1) + " (" + (success ? "success" : "failed") + ") " + " inserted to peer: " + flaschenPostInsertPeer.getNode());
        }

        if (success) {
            countSuccess++;
        } else {
            countFailed++;
        }

    }


    public void success() {
        success = true;
        done();
    }

    public static int getCountSuccess() {
        return countSuccess;
    }


    public static int getCountFailed() {
        return countFailed;
    }


    public static double getSuccessRate() {
        if (countSuccess + countFailed == 0) {
            return 0;
        }
        return (double) countSuccess / (double) (countSuccess + countFailed);
    }
}
