package im.redpanda.store;

import im.redpanda.core.KademliaId;
import im.redpanda.core.Node;
import im.redpanda.core.Server;
import org.jgrapht.graph.DefaultEdge;
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

    private final HashMap<KademliaId, Node> fastNodes;
    private final SimpleWeightedGraph<Node, DefaultEdge> nodeGraph;

    public NodeStore() {
        nodeGraph = Server.localSettings.getNodeGraph();

        ArrayList<Node> nodes = new ArrayList<Node>();
        for (Node node : nodeGraph.vertexSet()) {
            nodes.add(node);
        }
        nodeGraph.removeAllVertices(nodes);

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

        fastNodes = new HashMap<>();
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

    public HashMap<KademliaId, Node> getFastNodes() {


        if (fastNodes.size() < 10) {
            int toInsert = 10 - fastNodes.size();

            Set<Map.Entry<KademliaId, Node>> entriesSet = onHeap.entrySet();

            ArrayList<Map.Entry<KademliaId, Node>> entries = new ArrayList(entriesSet);

            Collections.sort(entries, Comparator.comparingInt(a -> -a.getValue().getScore()));

            for (Map.Entry<KademliaId, Node> o : entries) {
                System.out.println("v: " + o.getValue().getNodeId() + " " + o.getValue().getScore() + " " + o.getValue().getGmTestsSuccessful() + " " + o.getValue().getGmTestsFailed());
                if (!fastNodes.containsKey(o.getKey())) {
                    fastNodes.put(o.getKey(), o.getValue());
                    toInsert--;

                    nodeGraph.addVertex(o.getValue());
                    Node randomEdge = getRandomEdge(o.getValue());
                    if (randomEdge != null) {
                        DefaultEdge defaultEdge = nodeGraph.addEdge(o.getValue(), randomEdge);
                        nodeGraph.setEdgeWeight(defaultEdge, Math.random());
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

        printGraph();

        return fastNodes;
    }

    private void printGraph() {
        CSVExporter<Node, DefaultEdge> exporter = new CSVExporter<>(
                CSVFormat.EDGE_LIST
        );
        exporter.setParameter(CSVFormat.Parameter.EDGE_WEIGHTS, true);
        exporter.setVertexIdProvider(node -> node.toString());

        Writer writer = new StringWriter();
        exporter.exportGraph(nodeGraph, writer);
        System.out.println(writer.toString());
    }

    private void addRandomEdge() {

        Set<Node> kademliaIds = nodeGraph.vertexSet();

        ArrayList<Node> ids = new ArrayList<>(kademliaIds);


        boolean added = false;
        int count = 0;

        while (!added && count < 5) {

            Collections.shuffle(ids);
            Node a = ids.get(0);
            ids.remove(a);
            Collections.shuffle(ids);
            Node b = ids.get(0);
            ids.add(a);

            DefaultEdge defaultEdge = nodeGraph.addEdge(a, b);

            if (defaultEdge != null) {
                nodeGraph.setEdgeWeight(defaultEdge, 0);
                added = true;
            }

            count++;
        }


    }

    private Node getRandomEdge(Node exclude) {
        ArrayList<Node> nodes = new ArrayList<>(nodeGraph.vertexSet());
        nodes.remove(exclude);
        Collections.shuffle(nodes);
        if (nodes.isEmpty()) {
            return null;
        }
        return nodes.get(0);
    }

    public SimpleWeightedGraph<Node, DefaultEdge> getNodeGraph() {
        return nodeGraph;
    }
}
