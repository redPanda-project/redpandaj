package im.redpanda.jobs;

import im.redpanda.core.Node;
import im.redpanda.core.ServerContext;
import im.redpanda.kademlia.KadContent;
import im.redpanda.kademlia.nodeinfo.GMEntryPointModel;
import im.redpanda.kademlia.nodeinfo.NodeInfoModel;
import im.redpanda.store.NodeEdge;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

public class NodeInfoSetRefreshJob extends Job {
    private DefaultDirectedWeightedGraph<Node, NodeEdge> nodeGraph;

    public NodeInfoSetRefreshJob(ServerContext serverContext) {
        super(serverContext, Duration.ofSeconds(15).toMillis(), true, true);

    }

    @Override
    public void init() {
        nodeGraph = serverContext.getNodeStore().getNodeGraph();
    }

    @Override
    public void work() {

        setReRunDelay(Duration.ofMinutes(5).toMillis());

        ArrayList<NodeEdge> nodeEdges = new ArrayList<>();
        NodeInfoModel nodeInfoModel = new NodeInfoModel();
        serverContext.getNodeStore().getReadWriteLock().readLock().lock();
        try {
            nodeEdges.addAll(nodeGraph.incomingEdgesOf(serverContext.getServerNode()));
            Collections.sort(nodeEdges, Comparator.comparingDouble(nodeGraph::getEdgeWeight));
            Iterator<NodeEdge> iterator = nodeEdges.iterator();


            int cnt = 0;
            while (iterator.hasNext() && cnt < 5) {
                NodeEdge nodeEdge = iterator.next();
                System.out.println("best node for me: " + nodeGraph.getEdgeWeight(nodeEdge));
                Node edgeSource = nodeGraph.getEdgeSource(nodeEdge);

                nodeInfoModel.addEntryPoint(new GMEntryPointModel(edgeSource.getNodeId()));
                cnt++;
            }
        } finally {
            serverContext.getNodeStore().getReadWriteLock().readLock().unlock();
        }

        System.out.println("string to store: " + nodeInfoModel.export());

        byte[] payload = nodeInfoModel.export().getBytes();

        KadContent kadContent = new KadContent(serverContext.getNodeId().exportPublic(), payload);
        kadContent.signWith(serverContext.getNodeId());
        new KademliaInsertJob(serverContext, kadContent).start();

    }
}
