package im.redpanda.store;

import im.redpanda.core.KademliaId;
import im.redpanda.core.Node;
import im.redpanda.core.ServerContext;
import im.redpanda.jobs.PeerPerformanceTestGarlicMessageJob;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;
import org.jgrapht.graph.SimpleWeightedGraph;
import org.jgrapht.nio.Attribute;
import org.jgrapht.nio.DefaultAttribute;
import org.jgrapht.nio.csv.CSVExporter;
import org.jgrapht.nio.csv.CSVFormat;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import java.io.File;
import java.io.StringWriter;
import java.io.Writer;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class NodeStore {

    public static final long NODE_BLACKLISTED_FOR_GRAPH = 1000L * 60L * 60L * 2L;
    public static final int MAX_EDGES_IN_GRAPH = 500;
    public static final int MIN_EDGES_NEEDED_FOR_NODE_REMOVAL = 3;
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
    private final Map<Node, Long> nodeBlacklist;
    private final ServerContext serverContext;

    private NodeStore(ServerContext serverContext) {
        this.serverContext = serverContext;
        nodeBlacklist = new HashMap<>();
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
            System.out.println("warning, could not restore nodeGraph from local settings....");
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

    public void saveToDisk() {

        try {
            offHeap.clearWithExpire();
            onHeap.clearWithExpire();
            offHeap.clearWithExpire();
        } catch (Throwable e) {
            System.out.println("NodeStore may be broken here we have to close and reopen the store...");

            close();
            new File("data/nodeids" + serverContext.getPort() + ".mapdb").delete();
            serverContext.setNodeStore(new NodeStore(serverContext));
        }

//        System.out.println("save to disk: " + onHeap.size() + " " + offHeap.size() + " " + onDisk.size());

    }

    public void close() {

//        saveToDisk();

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

        removeNodeIfNoGoodLinkAvailable();

        int currentNodeCount = nodeGraph.vertexSet().size();

        if (currentNodeCount < 10) {
            int toInsert = 10 - currentNodeCount;

            ArrayList<Map.Entry<KademliaId, Node>> entries = new ArrayList(onHeap.entrySet());

            Collections.sort(entries, Comparator.comparingInt(a -> -a.getValue().getScore()));

            for (Map.Entry<KademliaId, Node> o : entries) {

                if (isNodeStillBlacklisted(o.getValue())) {
                    continue;
                }

//                System.out.println("v: " + o.getValue().getNodeId() + " " + o.getValue().getScore() + " " + o.getValue().getGmTestsSuccessful() + " " + o.getValue().getGmTestsFailed());
                if (!nodeGraph.containsVertex(o.getValue())) {
                    toInsert--;

                    nodeGraph.addVertex(o.getValue());
                    Node randomEdge = getRandomNode(o.getValue());
                    if (randomEdge != null) {
                        NodeEdge defaultEdge = nodeGraph.addEdge(o.getValue(), randomEdge);
                        nodeGraph.setEdgeWeight(defaultEdge, PeerPerformanceTestGarlicMessageJob.CUT_MID);
                    }

                    return;
                }
                if (toInsert == 0) {
                    break;
                }

            }

        }


        if (nodeGraph.edgeSet().size() < MAX_EDGES_IN_GRAPH) {
            addRandomEdgeIfWaitedEnough();
        }

//        printGraph();
    }

    private boolean isNodeStillBlacklisted(Node node) {
        return nodeBlacklist.containsKey(node) && System.currentTimeMillis() - nodeBlacklist.get(node) < NODE_BLACKLISTED_FOR_GRAPH;
    }

    public void clearNodeBlacklist() {
        nodeBlacklist.clear();
    }

    private void removeNodeIfNoGoodLinkAvailable() {

        Set<Node> nodes = nodeGraph.vertexSet();
        if (nodes.size() < 5) {
            return;
        }

        Node nodeToRemove = null;
        for (Node node : nodes) {

            boolean oneGoodLink = false;

            for (NodeEdge defaultEdge : nodeGraph.edgesOf(node)) {
                if (nodeGraph.getEdgeWeight(defaultEdge) <= PeerPerformanceTestGarlicMessageJob.LINK_FAILED) {
                    oneGoodLink = true;
                    break;
                }
            }

            if (!oneGoodLink && nodeGraph.edgesOf(node).size() >= MIN_EDGES_NEEDED_FOR_NODE_REMOVAL) {
                nodeToRemove = node;
                break;
            }


        }
        if (nodeToRemove != null) {
            nodeBlacklist.put(nodeToRemove, System.currentTimeMillis());
            nodeGraph.removeVertex(nodeToRemove);
            System.out.println("removed node since no good link available: " + nodeToRemove);
        }
    }


    public void printGraph() {
        CSVExporter<Node, NodeEdge> exporter = new CSVExporter<>(
                CSVFormat.MATRIX
        );
        exporter.setParameter(CSVFormat.Parameter.EDGE_WEIGHTS, true);
        exporter.setParameter(CSVFormat.Parameter.MATRIX_FORMAT_ZERO_WHEN_NO_EDGE, true);
        exporter.setVertexIdProvider(node -> node.toString());

        Writer writer = new StringWriter();
        exporter.exportGraph(nodeGraph, writer);
        System.out.println(String.format("Current Network Graph with weights representing the performance for garlic routing with %s edges.", nodeGraph.edgeSet().size()));
        System.out.println(writer);
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

        while (!added && count < 5) {

            Collections.shuffle(ids);
            Node a = ids.get(0);
            ids.remove(a);
            Collections.shuffle(ids);
            Node b = ids.get(0);
            ids.add(a);

            NodeEdge defaultEdge = nodeGraph.addEdge(a, b);

            if (defaultEdge != null) {
                nodeGraph.setEdgeWeight(defaultEdge, PeerPerformanceTestGarlicMessageJob.CUT_MID);
                added = true;
                System.out.println(String.format("added edge: %s -> %s", a.getNodeId(), b.getNodeId()));
            }

            count++;
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

        for (Node node : nodeBlacklist.keySet()) {
            System.out.println(node.toString());
        }


    }

    public void printAllNotBlacklisted() {



        for (Node node : nodeGraph.vertexSet()) {
//            if (nodeBlacklist.containsKey(node)) {
//                continue;
//            }
            System.out.println(node.toString() + " " + (nodeBlacklist.containsKey(node) ? "b" : ""));
        }


    }

    public void clearGraph() {
        nodeGraph = new DefaultDirectedWeightedGraph<>(NodeEdge.class);
    }
}
