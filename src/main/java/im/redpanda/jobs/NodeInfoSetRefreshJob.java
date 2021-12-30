package im.redpanda.jobs;

import im.redpanda.core.Node;
import im.redpanda.core.ServerContext;
import im.redpanda.crypt.Utils;
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
            nodeEdges.addAll(nodeGraph.incomingEdgesOf(serverContext.getNode()));
            Collections.sort(nodeEdges, Comparator.comparingDouble(nodeGraph::getEdgeWeight));
            Iterator<NodeEdge> iterator = nodeEdges.iterator();


            int cnt = 0;
            while (iterator.hasNext() && cnt < 10) {
                NodeEdge nodeEdge = iterator.next();
                double edgeWeight = nodeGraph.getEdgeWeight(nodeEdge);
                if (edgeWeight > 5) {
                    continue;
                }
                Node edgeSource = nodeGraph.getEdgeSource(nodeEdge);

                GMEntryPointModel gmEntryPointModel = new GMEntryPointModel(edgeSource.getNodeId());

                Node.ConnectionPoint connectionPoint = edgeSource.latestSeenConnectionPoint();
                if (connectionPoint != null && !Utils.isLocalAddress(connectionPoint.getIp())) {
                    gmEntryPointModel.setIp(connectionPoint.getIp());
                    gmEntryPointModel.setPort(connectionPoint.getPort());
                }
                nodeInfoModel.addEntryPoint(gmEntryPointModel);
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
