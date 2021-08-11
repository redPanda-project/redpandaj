package im.redpanda.store;

import im.redpanda.core.KademliaId;
import im.redpanda.core.Node;
import im.redpanda.core.Server;
import im.redpanda.jobs.PeerPerformanceTestGarlicMessageJob;
import org.jgrapht.graph.SimpleWeightedGraph;
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
    public static ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(2);

    /**
     * These sizes are upper limits of the different dbs, the main eviction should be done via a timeout
     * after a get operation since the eviction by size is random.
     */
    private static final long MAX_SIZE_ONHEAP = 50L * 1024L * 1024L;
    private static final long MAX_SIZE_OFFHEAP = 50L * 1024L * 1024L;
    private static final long MAX_SIZE_ONDISK = 300L * 1024L * 1024L;

    private final HTreeMap onHeap;
    private final HTreeMap offHeap;
    private final HTreeMap onDisk;
    private final DB dbonHeap;
    private final DB dboffHeap;
    private final DB dbDisk;

    private final SimpleWeightedGraph<Node, NodeEdge> nodeGraph;
    private final Map<Node, Long> nodeBlacklist;

    public NodeStore() {
        nodeBlacklist = new HashMap<>();

        if (Server.localSettings == null) {
            System.out.println("warning, could not restore nodeGraph from local settings....");
            nodeGraph = new SimpleWeightedGraph(NodeEdge.class);
        } else {
            nodeGraph = Server.localSettings.getNodeGraph();
        }

//        ArrayList<Node> nodes = new ArrayList<Node>();
//        for (Node node : nodeGraph.vertexSet()) {
//            nodes.add(node);
//        }
//        nodeGraph.removeAllVertices(nodes);

        dbonHeap = DBMaker
                .heapDB()
//                .closeOnJvmShutdown()
                .make();

        dboffHeap = DBMaker
                .memoryDirectDB()
//                .closeOnJvmShutdown()
                .make();

        dbDisk = DBMaker
                .fileDB("data/nodeids" + Server.MY_PORT + ".mapdb")
                .fileMmapEnableIfSupported()
//                .closeOnJvmShutdown()
                .checksumHeaderBypass()
                .make();

        onDisk = dbDisk
                .hashMap("nodeidsOnDisk")
                .expireStoreSize(MAX_SIZE_ONDISK)
                .expireExecutor(threadPool)
//                .expireAfterUpdate(60, TimeUnit.SECONDS) // no update since 14 days, not seen in this time
                .expireAfterGet(60, TimeUnit.DAYS)
                .createOrOpen();

        offHeap = dboffHeap
                .hashMap("nodeidsOffHeap")
                .expireStoreSize(MAX_SIZE_OFFHEAP)
                .expireOverflow(onDisk)
                .expireExecutor(threadPool)
                .expireAfterCreate()
                .expireAfterGet(60, TimeUnit.MINUTES)
                .create();

        onHeap = dbonHeap
                .hashMap("nodeidsOnHeap")
                .expireStoreSize(MAX_SIZE_ONHEAP)
                .expireOverflow(offHeap)
                .expireExecutor(threadPool)
                .expireAfterCreate()
                .expireAfterGet(15, TimeUnit.MINUTES)
                .create();
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
            new File("data/nodeids" + Server.MY_PORT + ".mapdb").delete();
            Server.nodeStore = new NodeStore();

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
        saveToDisk();
        return onDisk.size();
    }


    public void maintainNodes() {

        removeNodeIfNoGoodLinkAvailable();

        int currentNodeCount = nodeGraph.vertexSet().size();

        if (currentNodeCount < 10) {
            int toInsert = 10 - currentNodeCount;

            Set<Map.Entry<KademliaId, Node>> entriesSet = onHeap.entrySet();

            ArrayList<Map.Entry<KademliaId, Node>> entries = new ArrayList(entriesSet);

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


                }
                if (toInsert == 0) {
                    break;
                }

            }

        }


        if (nodeGraph.edgeSet().size() < 200) {
            addRandomEdge();
        }

//        printGraph();
    }

    private boolean isNodeStillBlacklisted(Node node) {
        return nodeBlacklist.containsKey(node) && System.currentTimeMillis() - nodeBlacklist.get(node) < NODE_BLACKLISTED_FOR_GRAPH;
    }

    private void removeNodeIfNoGoodLinkAvailable() {

        Node nodeToRemove = null;
        for (Node node : nodeGraph.vertexSet()) {

            boolean oneGoodLink = false;

            for (NodeEdge defaultEdge : nodeGraph.edgesOf(node)) {
                if (nodeGraph.getEdgeWeight(defaultEdge) > PeerPerformanceTestGarlicMessageJob.LINK_FAILED) {
                    oneGoodLink = true;
                    break;
                }
            }

            if (!oneGoodLink) {
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
                CSVFormat.EDGE_LIST
        );
        exporter.setParameter(CSVFormat.Parameter.EDGE_WEIGHTS, true);
        exporter.setVertexIdProvider(node -> node.toString());

        Writer writer = new StringWriter();
        exporter.exportGraph(nodeGraph, writer);
        System.out.println("Current Network Graph with weights representing the performance for garlic routing.");
        System.out.println(writer);
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

    public SimpleWeightedGraph<Node, NodeEdge> getNodeGraph() {
        return nodeGraph;
    }
}
