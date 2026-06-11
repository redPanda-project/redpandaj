package im.redpanda.flaschenpost;

import im.redpanda.core.KademliaId;
import im.redpanda.core.Node;
import im.redpanda.core.Peer;
import im.redpanda.core.PeerList;
import im.redpanda.core.ServerContext;
import im.redpanda.jobs.OhResolveJob;
import im.redpanda.kademlia.PeerComparator;
import im.redpanda.store.NodeEdge;
import java.util.ArrayList;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import lombok.extern.slf4j.Slf4j;

/**
 * MS02b cross-node OH delivery (Option A): a FlaschenpostPut whose {@code oh_id} is not registered
 * locally is forwarded toward the OH host node — resolved via the DHT announce record (see {@code
 * OhDht}) — with the {@code oh_id} preserved on every hop, so the destination node deposits into
 * the correct mailbox exactly as for a directly connected sender.
 *
 * <p>Loop protection: every forward increments {@code hop_count}; packets that arrive at the hop
 * limit are dropped. Delivery is best-effort (like the rest of the flaschenpost layer).
 */
@Slf4j
public final class OhForwarder {

  /** Maximum number of node-to-node forwards before a packet is dropped. */
  public static final int MAX_HOPS = 3;

  private OhForwarder() {}

  /**
   * Forwards a deposit for a non-local OH toward its host node. Resolution result and routing
   * happen asynchronously (the DHT lookup is randomized-delayed against profiling).
   *
   * @param hopCount the hop count the packet arrived with
   * @return {@code true} if forwarding was initiated (hop budget available), {@code false} if the
   *     packet was dropped at the hop limit
   */
  public static boolean forward(
      ServerContext serverContext, byte[] ohId, byte[] content, int hopCount) {
    if (hopCount >= MAX_HOPS) {
      log.debug("dropping FlaschenpostPut for unknown OH at hop limit {}", hopCount);
      return false;
    }
    OhResolveJob.resolve(
        serverContext,
        ohId,
        record -> {
          byte[] nodeIdBytes = record.getNodeId().toByteArray();
          routeToNode(serverContext, new KademliaId(nodeIdBytes), ohId, content, hopCount);
        },
        () -> log.debug("OH host resolution failed, dropping forwarded deposit"));
    return true;
  }

  /**
   * Routes the packet toward {@code targetNodeId}: directly if connected, otherwise via the
   * cheapest next hop in the weighted node graph (same selection as garlic routing).
   */
  static void routeToNode(
      ServerContext serverContext,
      KademliaId targetNodeId,
      byte[] ohId,
      byte[] content,
      int hopCount) {
    PeerList peerList = serverContext.getPeerList();

    Peer direct = peerList.get(targetNodeId);
    if (direct != null && direct.isConnected()) {
      GMParser.sendFpToPeer(direct, content, ohId, hopCount + 1);
      return;
    }

    TreeSet<Peer> candidates = new TreeSet<>(new PeerComparator(targetNodeId));
    Lock lock = peerList.getReadWriteLock().readLock();
    lock.lock();
    try {
      ArrayList<Peer> peerArrayList = peerList.getPeerArrayList();
      if (peerArrayList == null) {
        return;
      }
      for (Peer p : peerArrayList) {
        // same candidate filter as garlic routing: connected full nodes with known node id
        if (p.getNodeId() == null || !p.isConnected() || !p.hasNode() || p.isLightClient()) {
          continue;
        }
        candidates.add(p);
      }
    } finally {
      lock.unlock();
    }

    if (candidates.isEmpty()) {
      log.debug("no candidate peer to forward OH deposit toward {}", targetNodeId);
      return;
    }

    Node targetNode = serverContext.getNodeStore().get(targetNodeId);
    org.jgrapht.Graph<Node, NodeEdge> graph = serverContext.getNodeStore().getNodeGraph();
    GMParser.RouteSelection selection =
        GMParser.selectBestRoutePeer(
            graph, serverContext.getNode(), candidates, targetNode, GMParser.MAX_ROUTE_WEIGHT);

    if (selection.peer() != null) {
      GMParser.sendFpToPeer(selection.peer(), content, ohId, hopCount + 1);
      return;
    }

    // No graph route — greedy Kademlia fallback: next hop must be strictly closer to the target
    // than we are (forward progress), the hop limit bounds the worst case.
    Peer nearest = candidates.first();
    int ourDistance = targetNodeId.getDistance(serverContext.getNonce());
    int nearestDistance = targetNodeId.getDistance(nearest.getKademliaId());
    if (nearestDistance < ourDistance) {
      GMParser.sendFpToPeer(nearest, content, ohId, hopCount + 1);
    } else {
      log.debug("no peer closer to OH host {} than ourselves, dropping", targetNodeId);
    }
  }
}
