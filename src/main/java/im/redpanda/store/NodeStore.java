package im.redpanda.store;

import im.redpanda.core.*;
import im.redpanda.jobs.PeerPerformanceTestGarlicMessageJob;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class NodeStore {

    public static final long NODE_BLACKLISTED_FOR_GRAPH = 1000L * 60L * 60L * 24L;
    public static final int MAX_EDGES_IN_GRAPH = 500;
    public static final int MIN_EDGES_NEEDED_FOR_NODE_REMOVAL = 5;
    public static final int MAX_NODES_FOR_GRAPH = 20;
    public static ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(2);

    /**
     * These sizes are upper limits of the different dbs, the main eviction should be done via a timeout
     * after a get operation since the eviction by size is random.
     */
    private static final long MAX_SIZE_ONHEAP = 50L * 1024L * 1024L;
    private static final long MAX_SIZE_OFFHEAP = 50L * 1024L * 1024L;
    private static final long MAX_SIZE_ONDISK = 300L * 1024L * 1024L;

    private HTreeMap onHeap;
    private HTreeMap offHeap;
    private HTreeMap onDisk;
    private DB dbonHeap;
    private DB dboffHeap;
    private DB dbDisk;

    private DefaultDirectedWeightedGraph<Node, NodeEdge> nodeGraph;
    private long lastTimeEdgeAdded = 0;
    private final ServerContext serverContext;
    private final Random random = new Random();

    private NodeStore(ServerContext serverContext) {
        this.serverContext = serverContext;
        nodeGraph = new DefaultDirectedWeightedGraph<>(NodeEdge.class);
    }

    public static NodeStore buildWithDiskCache(ServerContext serverContext) {

        NodeStore nodeStore = new NodeStore(serverContext);

        if (serverContext.getLocalSettings() == null) {
            System.out.println("warning, could not restore nodeGraph from local settings....");
        } else {
            nodeStore.nodeGraph = serverContext.getLocalSettings().getNodeGraph();
        }

        nodeStore.dbonHeap = DBMaker
                .heapDB()
//                .closeOnJvmShutdown()
                .make();

        nodeStore.dboffHeap = DBMaker
                .memoryDirectDB()
//                .closeOnJvmShutdown()
                .make();

        nodeStore.dbDisk = DBMaker
                .fileDB("data/nodeids" + serverContext.getPort() + ".mapdb")
                .fileMmapEnableIfSupported()
//                .closeOnJvmShutdown()
                .checksumHeaderBypass()
                .make();

        nodeStore.onDisk = nodeStore.dbDisk
                .hashMap("nodeidsOnDisk")
                .expireStoreSize(MAX_SIZE_ONDISK)
                .expireExecutor(threadPool)
//                .expireAfterUpdate(60, TimeUnit.SECONDS) // no update since 14 days, not seen in this time
                .expireAfterGet(60, TimeUnit.DAYS)
                .createOrOpen();

        nodeStore.offHeap = nodeStore.dboffHeap
                .hashMap("nodeidsOffHeap")
                .expireStoreSize(MAX_SIZE_OFFHEAP)
                .expireOverflow(nodeStore.onDisk)
                .expireExecutor(threadPool)
                .expireAfterCreate()
                .expireAfterGet(60, TimeUnit.MINUTES)
                .create();

        nodeStore.onHeap = nodeStore.dbonHeap
                .hashMap("nodeidsOnHeap")
                .expireStoreSize(MAX_SIZE_ONHEAP)
                .expireOverflow(nodeStore.offHeap)
                .expireExecutor(threadPool)
                .expireAfterCreate()
                .expireAfterGet(15, TimeUnit.MINUTES)
                .create();

        return nodeStore;
    }

    public static NodeStore buildWithMemoryCacheOnly(ServerContext serverContext) {
        NodeStore nodeStore = new NodeStore(serverContext);

        if (serverContext.getLocalSettings() == null) {
            Log.put("warning, could not restore nodeGraph from local settings....", 5);
        } else {
            nodeStore.nodeGraph = serverContext.getLocalSettings().getNodeGraph();
        }

        nodeStore.dbonHeap = DBMaker
                .heapDB()
                .make();

        nodeStore.onHeap = nodeStore.dbonHeap
                .hashMap("nodeidsOnHeap")
                .expireStoreSize(MAX_SIZE_ONHEAP)
                .expireExecutor(threadPool)
                .expireAfterCreate()
                .expireAfterGet(15, TimeUnit.HOURS)
                .create();

        return nodeStore;
    }

    public void put(KademliaId kademliaId, Node node) {
        onHeap.put(kademliaId, node);
    }

    public Node get(KademliaId kademliaId) {
        try {
            return (Node) onHeap.get(kademliaId);
        } catch (Exception e) {
            e.printStackTrace();
            onDisk.clear();
            return null;
        }
    }

    public void remove(KademliaId kademliaId) {
        onHeap.remove(kademliaId);
    }

    public void saveToDisk() {

        try {
            offHeap.clearWithExpire();
            onHeap.clearWithExpire();
            offHeap.clearWithExpire();
        } catch (Throwable e) {
            System.out.println("NodeStore may be broken here we have to close and reopen the store...");

            close();
            Path path = Path.of(String.format("data/nodeids%s.mapdb", serverContext.getPort()));
            try {
                Files.delete(path);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            serverContext.setNodeStore(new NodeStore(serverContext));
        }

    }

    public void close() {
        onHeap.close();
        offHeap.close();
        onDisk.close();

        dbonHeap.close();
        dboffHeap.close();
        dbDisk.close();
    }

    /**
     * Writes all to disk and then reads the size from the disk db.
     *
     * @return
     */
    public int size() {
        if (onDisk == null) {
            return onHeap.size();
        }
        saveToDisk();
        return onDisk.size();
    }


    public void maintainNodes() {

        decayRandomEdge();

        addServerEdges();

        removeNodeIfNoGoodLinkAvailable();

        removeBadScoredNode();

        addRandomNodeToGraph();

        if (nodeGraph.edgeSet().size() < MAX_EDGES_IN_GRAPH) {
            addRandomEdgeIfWaitedEnough();
        }
    }

    private void removeBadScoredNode() {
        ArrayList<Node> nodes = new ArrayList<>(nodeGraph.vertexSet());
        for (Node node : nodes) {
            if (node.equals(serverContext.getServerNode())) {
                continue;
            }

            if (node.getNodeId().getKeyPair() == null) {
                //this may be an old own server id...
                remove(node.getNodeId().getKademliaId());
            }

            if (node.getScore() < -20) {
                int veryGoodLinks = 0;
                for (NodeEdge nodeEdge : nodeGraph.outgoingEdgesOf(node)) {
                    if (nodeGraph.getEdgeWeight(nodeEdge) < 3) {
                        veryGoodLinks++;
                    }
                }
                if (veryGoodLinks <= 3) {
                    removeNodeFromGraphAndBlacklist(node);
                    System.out.println(String.format("remove node %s due to bad score of %s, very good links only %s", node, node.getScore(), veryGoodLinks));
                }
            }

        }


    }

    private void decayRandomEdge() {
        ArrayList<NodeEdge> nodeEdges = new ArrayList<>(nodeGraph.edgeSet());
        if (nodeEdges.size() < 4) {
            return;
        }
        NodeEdge randomEdge = nodeEdges.get(random.nextInt(nodeEdges.size()));
        nodeGraph.setEdgeWeight(randomEdge, nodeGraph.getEdgeWeight(randomEdge) + 1);
    }

    private void addServerEdges() {
        Node serverNode = serverContext.getServerNode();
        if (nodeGraph.outgoingEdgesOf(serverNode).size() < 5 || nodeGraph.incomingEdgesOf(serverNode).size() < 5) {

            for (Peer peer : serverContext.getPeerList().getPeerArrayList()) {
                if (!peer.isConnected() || peer.getNode() == null || !nodeGraph.containsVertex(peer.getNode())) {
                    continue;
                }
                nodeGraph.addEdge(serverNode, peer.getNode());
                nodeGraph.addEdge(peer.getNode(), serverNode);
            }


        }

    }

    private void addRandomNodeToGraph() {
        int currentNodeCount = nodeGraph.vertexSet().size();

        if (currentNodeCount < MAX_NODES_FOR_GRAPH) {

            ArrayList<Map.Entry<KademliaId, Node>> entries = new ArrayList(onHeap.entrySet());

            Collections.sort(entries, Comparator.comparingInt(a -> -a.getValue().getScore()));

            for (Map.Entry<KademliaId, Node> o : entries) {
                Node nodeToAdd = o.getValue();

                if (nodeToAdd.isBlacklisted()) {
                    continue;
                }

                if (!nodeGraph.containsVertex(nodeToAdd)) {
                    addNodeWithInitialEdges(nodeToAdd);
                    break;
                }
            }

        }
    }

    private void addNodeWithInitialEdges(Node nodeToAdd) {
        nodeGraph.addVertex(nodeToAdd);
        Node randomEdge = getRandomNode(nodeToAdd);
        if (randomEdge != null) {
            NodeEdge defaultEdge = nodeGraph.addEdge(nodeToAdd, randomEdge);
            nodeGraph.setEdgeWeight(defaultEdge, PeerPerformanceTestGarlicMessageJob.CUT_HARD);
        }
    }

    private void removeNodeIfNoGoodLinkAvailable() {

        List<Node> nodes = new ArrayList(nodeGraph.vertexSet());
        nodes.remove(serverContext.getServerNode());
        if (nodes.size() < 4) {
            return;
        }

        Node nodeToRemove = null;
        for (Node node : nodes) {

            boolean oneGoodLink = isOneGoodLinkAvailable(node);

            if (!oneGoodLink && nodeGraph.edgesOf(node).size() >= MIN_EDGES_NEEDED_FOR_NODE_REMOVAL) {
                nodeToRemove = node;
                break;
            }


        }
        if (nodeToRemove != null) {
            removeNodeFromGraphAndBlacklist(nodeToRemove);
        }
    }

    private void removeNodeFromGraphAndBlacklist(Node nodeToRemove) {
        nodeToRemove.touchBlacklisted();
        nodeGraph.removeVertex(nodeToRemove);
        System.out.println("removed node since no good link available: " + nodeToRemove);
    }

    private boolean isOneGoodLinkAvailable(Node node) {
        for (NodeEdge edge : nodeGraph.edgesOf(node)) {
            if (nodeGraph.getEdgeWeight(edge) <= PeerPerformanceTestGarlicMessageJob.LINK_FAILED) {
                return true;
            }
        }
        return false;
    }


    private void addRandomEdgeIfWaitedEnough() {
        boolean allEdgesGood = true;
        for (NodeEdge edge : nodeGraph.edgeSet()) {
            if (nodeGraph.getEdgeWeight(edge) < 5) {
                allEdgesGood = false;
                break;
            }
        }

        if (allEdgesGood || System.currentTimeMillis() - lastTimeEdgeAdded > 1000L * 10L) {
            addRandomEdge();
            lastTimeEdgeAdded = System.currentTimeMillis();
        }

    }

    private void addRandomEdge() {
        Set<Node> nodes = nodeGraph.vertexSet();

        if (nodes.size() < 2) {
            return;
        }

        ArrayList<Node> ids = new ArrayList<>(nodes);


        boolean added = false;
        int count = 0;

        while (!added && count < 10) {
            count++;

            Collections.shuffle(ids);
            Node nodeFrome = ids.get(0);
            ids.remove(nodeFrome);
            Collections.shuffle(ids);
            Node nodeTo = ids.get(0);
            ids.add(nodeFrome);
            ids.add(nodeTo);

            if (nodeFrome.equals(nodeTo)) {
                continue;
            }

            NodeEdge defaultEdge = nodeGraph.addEdge(nodeFrome, nodeTo);

            if (defaultEdge != null) {
                nodeGraph.setEdgeWeight(defaultEdge, PeerPerformanceTestGarlicMessageJob.CUT_HARD);
                added = true;
                System.out.println(String.format("added edge: %s -> %s", nodeFrome.getNodeId(), nodeTo.getNodeId()));
            }


        }


    }

    private Node getRandomNode(Node exclude) {
        ArrayList<Node> nodes = new ArrayList<>(nodeGraph.vertexSet());
        nodes.remove(exclude);
        Collections.shuffle(nodes);
        if (nodes.isEmpty()) {
            return null;
        }
        return nodes.get(0);
    }

    public DefaultDirectedWeightedGraph<Node, NodeEdge> getNodeGraph() {
        return nodeGraph;
    }

    public void printBlacklist() {
        for (Object value : onHeap.getValues()) {
            Node node = (Node) value;
            if (node.isBlacklisted()) {
                System.out.println(node);
            }
        }
    }

    public void printAllNotBlacklisted() {


        for (Node node : nodeGraph.vertexSet()) {
//            if (nodeBlacklist.containsKey(node)) {
//                continue;
//            }
            System.out.println(node.toString() + " " + (node.isBlacklisted() ? "b" : ""));
        }


    }

    public void clearGraph() {
        nodeGraph = new DefaultDirectedWeightedGraph<>(NodeEdge.class);
        nodeGraph.addVertex(serverContext.getServerNode());
    }

    public void clearNodeBlacklist() {
        for (Object value : onHeap.getValues()) {
            Node node = (Node) value;
            node.resetBlacklisted();
            node.setGmTestsSuccessful(0);
            node.setGmTestsFailed(0);
        }
    }
}
