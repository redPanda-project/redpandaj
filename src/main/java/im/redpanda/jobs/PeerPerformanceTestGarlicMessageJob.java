package im.redpanda.jobs;

import im.redpanda.core.Command;
import im.redpanda.core.Node;
import im.redpanda.core.NodeId;
import im.redpanda.core.Peer;
import im.redpanda.core.Server;
import im.redpanda.core.ServerContext;
import im.redpanda.flaschenpost.GMAck;
import im.redpanda.flaschenpost.GarlicMessage;
import im.redpanda.store.NodeEdge;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class PeerPerformanceTestGarlicMessageJob extends Job {

    public static final int TEST_HOPS_MAX = 8;
    public static final int TEST_HOPS_MIN = 2;

    public static final double LINK_FAILED = 12;
    public static final double CUT_HARD = 12;
    public static final int CUT_MID = 10;
    public static final double CUT_LOW = 3;
    public static final int MAX_WEIGHT = 15;
    public static final int MIN_WEIGHT = 1;
    public static final float DELTA_SUCCESS = -1;
    public static final float DELTA_FAIL = 0.4f;
    public static final long JOB_TIMEOUT = 1000L * 5L;
    public static final long WAIT_CURT_HARD = 1000L * 60 * 4;
    public static final long WAIT_CUT_MID = 1000L * 30;
    public static final long WAIT_CUT_LOW = 1000L * 15;

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";

    private static final AtomicInteger countSuccess = new AtomicInteger();
    private static final AtomicInteger countFailed = new AtomicInteger();

    ArrayList<Node> nodes;
    boolean success = false;
    private Peer flaschenPostInsertPeer;
    boolean includeReversedPath = false;

    public PeerPerformanceTestGarlicMessageJob(ServerContext serverContext) {
        super(serverContext, 2500L);
        nodes = new ArrayList<>();
    }

    public byte[] calculateNestedGarlicMessages(List<Node> nodes, int jobId) {
        // lets target to ourselves without the private key!
        NodeId targetId = NodeId.importPublic(serverContext.getNodeId().exportPublic());

        GMAck gmAck = new GMAck(jobId);

        GarlicMessage currentLayer = new GarlicMessage(serverContext, targetId);
        currentLayer.addGMContent(gmAck);

        // omit own node at position 0 and node.size
        for (int i = nodes.size() - 2; i > 0; i--) {
            Node node = nodes.get(i);
            GarlicMessage newLayer = new GarlicMessage(serverContext, node.getNodeId());
            newLayer.addGMContent(currentLayer);

            currentLayer = newLayer;
        }

        return currentLayer.getContent();
    }

    @Override
    public void init() {
        serverContext.getNodeStore().getReadWriteLock().writeLock().lock();
        try {
            if (calculatePathOrAbort()) {
                return;
            }
        } finally {
            serverContext.getNodeStore().getReadWriteLock().writeLock().unlock();
        }
        byte[] content = calculateNestedGarlicMessages(this.nodes, getJobId());

        flaschenPostInsertPeer.getNode().cleanChecks();

        flaschenPostInsertPeer.getWriteBufferLock().lock();
        try {
            var putMsg = im.redpanda.proto.FlaschenpostPut.newBuilder()
                    .setContent(com.google.protobuf.ByteString.copyFrom(content))
                    .build();
            byte[] data = putMsg.toByteArray();

            flaschenPostInsertPeer.getWriteBuffer().put(Command.FLASCHENPOST_PUT);
            flaschenPostInsertPeer.getWriteBuffer().putInt(data.length);
            flaschenPostInsertPeer.getWriteBuffer().put(data);
            flaschenPostInsertPeer.setWriteBufferFilled();
        } finally {
            flaschenPostInsertPeer.getWriteBufferLock().unlock();
        }

    }

    private boolean calculatePathOrAbort() {
        DefaultDirectedWeightedGraph<Node, NodeEdge> nodeGraph = serverContext.getNodeStore().getNodeGraph();
        if (nodeGraph.vertexSet().isEmpty()) {
            super.done();
            return true;
        }

        // nodes = hops + 1
        int garlicSequenceLength = TEST_HOPS_MIN + Server.secureRandom.nextInt(TEST_HOPS_MAX - TEST_HOPS_MIN) + 1;
        if (nodeGraph.vertexSet().size() < garlicSequenceLength) {
            garlicSequenceLength = nodeGraph.vertexSet().size();
        }

        if (getSuccessRate() < 0.8) {
            garlicSequenceLength = 1;
        }

        Node startingNode = serverContext.getNode();

        this.nodes = new ArrayList<>();
        Node currentNode = startingNode;
        nodes.add(currentNode);
        int currentLength = 0;
        double pathWeight = 0;

        while (currentLength < garlicSequenceLength) {
            ArrayList<NodeEdge> edges = new ArrayList<>(nodeGraph.outgoingEdgesOf(currentNode));
            Collections.sort(edges);
            for (NodeEdge edge : edges) {
                // if an edge is bad we should only test it rarely

                if (edge.isInLastTimeCheckWindow() || dismissCheckByTimeoutIfEdgeQualityBad(nodeGraph, edge)) {
                    continue;
                }

                if (currentLength == 0) {
                    Node targetNode = serverContext.getNodeStore().getNodeGraph().getEdgeTarget(edge);
                    Peer targetPeer = serverContext.getPeerList().get(targetNode.getNodeId().getKademliaId());
                    if (targetPeer == null || !targetPeer.isConnected()) {
                        continue;
                    }
                }

                if (!nodeGraph.containsEdge(edge)) {
                    continue;
                }
                Node target = nodeGraph.getEdgeTarget(edge);

                if (nodes.contains(target)) {
                    continue;
                }

                currentNode = target;
                nodes.add(currentNode);
                edge.touchLastTimeCheckStarted();
                currentNode.cleanChecks();
                pathWeight += nodeGraph.getEdgeWeight(edge);
                break;
            }

            currentLength++;

            if (pathWeight > 10) {
                if (currentLength >= 2) {
                    addReversePath();
                }
                break;
            }

        }

        if (nodes.size() < 2) {
            super.done();
            return true;
        }

        nodes.add(serverContext.getNode());

        flaschenPostInsertPeer = serverContext.getPeerList().get(nodes.get(1).getNodeId().getKademliaId());

        if (flaschenPostInsertPeer == null || flaschenPostInsertPeer.getNode() == null) {
            super.done();
            return true;
        }
        return false;
    }

    private void addReversePath() {
        includeReversedPath = true;
        for (int currentNodeIndex = nodes.size() - 1; currentNodeIndex > 1; currentNodeIndex--) {
            Node currentNode = nodes.get(currentNodeIndex);
            Node targetNode = nodes.get(currentNodeIndex - 1);
            serverContext.getNodeStore().getNodeGraph().addEdge(currentNode, targetNode);
            NodeEdge edge = serverContext.getNodeStore().getNodeGraph().getEdge(currentNode, targetNode);
            edge.touchLastTimeCheckStarted();
            nodes.add(targetNode);
        }
    }

    private boolean dismissCheckByTimeoutIfEdgeQualityBad(DefaultDirectedWeightedGraph<Node, NodeEdge> nodeGraph,
            NodeEdge edge) {
        if (edge.isLastCheckFailed()) {
            double edgeWeight = nodeGraph.getEdgeWeight(edge);
            if (edgeWeight >= MAX_WEIGHT) {
                return System.currentTimeMillis() - edge.getTimeLastCheckFailed() < WAIT_CURT_HARD
                        + rand.nextInt((int) Duration.ofSeconds(60).toMillis());
            } else if (edgeWeight > CUT_HARD) {
                return System.currentTimeMillis() - edge.getTimeLastCheckFailed() < WAIT_CURT_HARD;
            } else if (edgeWeight > CUT_MID) {
                return System.currentTimeMillis() - edge.getTimeLastCheckFailed() < WAIT_CUT_MID;
            } else if (edgeWeight > CUT_LOW) {
                return System.currentTimeMillis() - edge.getTimeLastCheckFailed() < WAIT_CUT_LOW;
            }
        }
        return false;
    }

    @Override
    public void work() {
        if (getEstimatedRuntime() > JOB_TIMEOUT) {

            if (flaschenPostInsertPeer != null && flaschenPostInsertPeer.getNode() != null) {
                done();
            }
        }
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
            flaschenPostInsertPeer.getNode().seen();

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
                pathString += " -(" + "%.0f".formatted(newWeight) + ")-> " + node;
            } else {
                pathString += node;
            }
            nodeBefore = node;
        }

        // if (!success) {
        System.out.println((success ? ANSI_GREEN : ANSI_RED) + "path: " + pathString + " hops: " + (nodes.size() - 1)
                + " inserted to peer: " + flaschenPostInsertPeer.getNode() + ANSI_RESET
                + (includeReversedPath ? " REVERSED" : ""));
        // }

        if (success) {
            countSuccess.incrementAndGet();
        } else {
            countFailed.incrementAndGet();
        }

    }

    public void success() {
        success = true;
        done();
    }

    public static int getCountSuccess() {
        return countSuccess.get();
    }

    public static int getCountFailed() {
        return countFailed.get();
    }

    public static double getSuccessRate() {
        if (countSuccess.get() + countFailed.get() == 0) {
            return 0;
        }
        return (double) countSuccess.get() / (double) (countSuccess.get() + countFailed.get());
    }

    public static void decayRates() {
        int newCountSuccess = countSuccess.decrementAndGet();
        if (newCountSuccess < 0) {
            countSuccess.set(0);
        }
        int newCountFailed = countFailed.decrementAndGet();
        if (newCountFailed < 0) {
            countFailed.set(0);
        }
    }
}
