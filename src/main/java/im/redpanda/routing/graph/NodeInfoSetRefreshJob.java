package im.redpanda.routing.graph;

import im.redpanda.core.ServerContext;
import im.redpanda.dht.KadContent;
import im.redpanda.dht.KademliaInsertJob;
import im.redpanda.dht.nodeinfo.GMEntryPointModel;
import im.redpanda.dht.nodeinfo.NodeInfoModel;
import im.redpanda.identity.crypt.Utils;
import im.redpanda.jobs.Job;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;

public class NodeInfoSetRefreshJob extends Job {

  public NodeInfoSetRefreshJob(ServerContext serverContext) {
    super(serverContext, Duration.ofSeconds(15).toMillis(), true, true);
  }

  @Override
  public void init() {
    // Deliberately empty: the node graph must NOT be cached here. See getGoodEntryPoints().
  }

  @Override
  public void work() {

    setReRunDelay(Duration.ofMinutes(5).toMillis());

    NodeInfoModel nodeInfoModel = new NodeInfoModel();
    nodeInfoModel.addAllEntryPoints(getGoodEntryPoints());
    nodeInfoModel.setUptime(
        serverContext.getLocalSettings().getSystemUpTimeData().getUptimePercentAsInt());

    if (serverContext.getOutboundService() != null) {
      nodeInfoModel.addService("outbound_v1");
    }

    System.out.println("string to store: " + nodeInfoModel.export());

    byte[] payload = nodeInfoModel.export().getBytes();

    KadContent kadContent = new KadContent(serverContext.getNodeId().exportPublic(), payload);
    kadContent.signWith(serverContext.getNodeId());
    new KademliaInsertJob(serverContext, kadContent).start();
  }

  private List<GMEntryPointModel> getGoodEntryPoints() {
    ArrayList<NodeEdge> nodeEdges = new ArrayList<>();
    ArrayList<GMEntryPointModel> gmEntryPointModels = new ArrayList<>();
    // Resolve the NodeStore ONCE and take the lock and the graph from that same instance.
    // NodeStore.saveToDisk() replaces serverContext's store on its recovery path (NodeStore:205),
    // and a new NodeStore brings both a new readWriteLock (:60, final per instance) and a new empty
    // nodeGraph (:64). Two bugs follow from mixing instances, and this job used to have both:
    //
    //   - unlocking via a re-read getNodeStore() releases a lock this thread never took
    //     (IllegalMonitorStateException) and leaks the original read hold forever;
    //   - reading the graph from a field captured in init() — which Job runs exactly once
    //     (Job:64-67), while this job is permanent and re-runs every 5 minutes — pairs the new
    //     store's lock with the OLD store's graph, so the lock guards nothing that is being
    //     iterated, and the job keeps publishing entry points from a dead graph forever.
    //
    // Same shape as PeerPerformanceTestGarlicMessageJob:355-360 and OhForwarder.selectNextPeer().
    NodeStore nodeStore = serverContext.getNodeStore();
    Lock graphLock = nodeStore.getReadWriteLock().readLock();
    graphLock.lock();
    try {
      DefaultDirectedWeightedGraph<Node, NodeEdge> nodeGraph = nodeStore.getNodeGraph();
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

        gmEntryPointModels.add(gmEntryPointModel);
        cnt++;
      }
    } finally {
      graphLock.unlock();
    }

    return gmEntryPointModels;
  }
}
