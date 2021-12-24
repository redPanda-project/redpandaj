package im.redpanda.jobs;

import im.redpanda.core.Node;
import im.redpanda.core.ServerContext;
import im.redpanda.kademlia.KadContent;
import im.redpanda.kademlia.nodeinfo.GMEntryPointModel;
import im.redpanda.kademlia.nodeinfo.NodeInfoModel;
import im.redpanda.store.NodeEdge;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;

import java.time.Duration;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

public class NodeInfoSetRefreshJob extends Job {
    private final DefaultDirectedWeightedGraph<Node, NodeEdge> nodeGraph;

    public NodeInfoSetRefreshJob(ServerContext serverContext) {
        super(serverContext, Duration.ofSeconds(15).toMillis(), true);
        nodeGraph = serverContext.getNodeStore().getNodeGraph();
    }

    @Override
    public void init() {
        //increase further insert wait delay
        setReRunDelay(Duration.ofMinutes(5).toMillis());
    }

    @Override
    public void work() {

        TreeSet<NodeEdge> nodeEdges = new TreeSet<>(Comparator.comparingDouble(nodeGraph::getEdgeWeight));
        nodeEdges.addAll(nodeGraph.incomingEdgesOf(serverContext.getServerNode()));
        Iterator<NodeEdge> iterator = nodeEdges.iterator();

        NodeInfoModel nodeInfoModel = new NodeInfoModel();

        int cnt = 0;
        while (iterator.hasNext() && cnt < 3) {
            NodeEdge nodeEdge = iterator.next();
            System.out.println("best node for me: " + nodeGraph.getEdgeWeight(nodeEdge));
            Node edgeSource = nodeGraph.getEdgeSource(nodeEdge);

            nodeInfoModel.addEntryPoint(new GMEntryPointModel(edgeSource.getNodeId()));
            cnt++;
        }

        System.out.println("string to store: " + nodeInfoModel.export());

        byte[] payload = nodeInfoModel.export().getBytes();


        KadContent kadContent = new KadContent(serverContext.getNodeId().exportPublic(), payload);
        kadContent.signWith(serverContext.getNodeId());
        new KademliaInsertJob(serverContext, kadContent).start();

    }
}
