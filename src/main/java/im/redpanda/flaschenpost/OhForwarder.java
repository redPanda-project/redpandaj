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
    return forward(serverContext, ohId, content, hopCount, null);
  }

  /**
   * Forwards a deposit for a non-local OH toward its host node, preserving an MS05 reverse-garlic
   * session tag (or {@code null}/empty for untagged deposits). Resolution result and routing happen
   * asynchronously (the DHT lookup is randomized-delayed against profiling).
   *
   * @param hopCount the hop count the packet arrived with
   * @return {@code true} if forwarding was initiated (hop budget available), {@code false} if the
   *     packet was dropped at the hop limit
   */
  public static boolean forward(
      ServerContext serverContext, byte[] ohId, byte[] content, int hopCount, byte[] sessionTag) {
    return forward(serverContext, ohId, content, hopCount, sessionTag, null);
  }

  /**
   * Forwards a deposit for a non-local OH toward its host node, additionally preserving an MS06
   * return-path block (or {@code null}/empty when no R-ACK was requested) so the host node can send
   * the {@code RoutingAck} after its deposit decision. See {@link #forward(ServerContext, byte[],
   * byte[], int, byte[])} for the forwarding semantics.
   *
   * @param hopCount the hop count the packet arrived with
   * @return {@code true} if forwarding was initiated (hop budget available), {@code false} if the
   *     packet was dropped at the hop limit
   */
  public static boolean forward(
      ServerContext serverContext,
      byte[] ohId,
      byte[] content,
      int hopCount,
      byte[] sessionTag,
      byte[] returnPath) {
    if (hopCount >= MAX_HOPS) {
      // Hop-limit drop. This is the ONLY drop point that returns false, i.e. reports the drop
      // back to the synchronous caller. The caller (InboundCommandProcessor#handleFlaschenpostPut)
      // is still able to answer the peer here and sends the HANDLE_EXPIRED R-ACK itself, so we
      // MUST NOT ack here — doing so would double-ack the packet. All drops that happen inside the
      // asynchronous resolve callbacks below (resolve failure, no route) are the opposite case: the
      // caller has already answered OK and can no longer ack, so those paths ack here instead.
      log.debug("dropping FlaschenpostPut for unknown OH at hop limit {}", hopCount);
      return false;
    }
    // Parse the MS06 return path once for the R-ACK. The raw bytes are still forwarded verbatim on
    // the wire (routeToNode below); this parsed copy only decides whether an async drop can ack.
    // A malformed/absent block yields null → keep the pre-MS06 silent-drop behavior.
    ReturnPath ackPath = parseAckPath(returnPath);
    OhResolveJob.resolve(
        serverContext,
        ohId,
        record -> {
          byte[] nodeIdBytes = record.getNodeId().toByteArray();
          routeToNode(
              serverContext,
              new KademliaId(nodeIdBytes),
              ohId,
              content,
              hopCount,
              sessionTag,
              returnPath,
              ackPath);
        },
        () -> onResolveFailed(serverContext, ackPath));
    return true;
  }

  /**
   * Parses the raw return-path bytes into a {@link ReturnPath} for R-ACK purposes. Returns {@code
   * null} when no return path was carried or the block is malformed — in both cases the caller
   * keeps the silent-drop behavior (there is nobody safe to ack).
   */
  static ReturnPath parseAckPath(byte[] returnPath) {
    if (returnPath == null || returnPath.length == 0) {
      return null;
    }
    return ReturnPath.parseExact(returnPath);
  }

  /**
   * Async resolve-failure drop: the sender was already answered OK by the caller, so without this
   * R-ACK it would wait out its full R-ACK timeout. Sends exactly one {@code HANDLE_EXPIRED} ack
   * when the deposit carried a valid return path (see the double-ack invariant in {@link
   * #forward(ServerContext, byte[], byte[], int, byte[], byte[])}); otherwise a plain silent drop.
   */
  static void onResolveFailed(ServerContext serverContext, ReturnPath ackPath) {
    log.debug("OH host resolution failed, dropping forwarded deposit");
    if (ackPath != null) {
      RoutingAckSender.send(serverContext, ackPath, RoutingAckSender.STATUS_HANDLE_EXPIRED);
    }
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
      int hopCount,
      byte[] sessionTag,
      byte[] returnPath,
      ReturnPath ackPath) {
    Peer nextPeer = selectNextPeer(serverContext, targetNodeId);
    if (nextPeer != null) {
      GMParser.sendFpToPeer(nextPeer, content, ohId, hopCount + 1, sessionTag, returnPath);
      return;
    }
    // Resolved the host node but found no usable next hop: another async drop after the caller
    // already answered OK. Ack HANDLE_EXPIRED (best available status) so the sender does not wait
    // out its timeout. This runs only on the resolve-SUCCESS branch, mutually exclusive with the
    // resolve-failure ack in onResolveFailed, so still exactly one ack per packet.
    if (ackPath != null) {
      RoutingAckSender.send(serverContext, ackPath, RoutingAckSender.STATUS_HANDLE_EXPIRED);
    }
  }

  /**
   * Selects the next peer toward {@code targetNodeId}: the target itself if directly connected,
   * otherwise the cheapest next hop in the weighted node graph, otherwise the greedy Kademlia step
   * (only if it makes strict forward progress). Shared by the MS02b OH forwarding and the MS04
   * Flaschenpost v2 relay routing.
   *
   * @return the selected peer or {@code null} if no usable route exists (best-effort drop)
   */
  static Peer selectNextPeer(ServerContext serverContext, KademliaId targetNodeId) {
    PeerList peerList = serverContext.getPeerList();

    Peer direct = peerList.get(targetNodeId);
    if (direct != null && direct.isConnected()) {
      return direct;
    }

    // tie-break equal XOR distances by KademliaId so the TreeSet never collapses distinct peers
    TreeSet<Peer> candidates =
        new TreeSet<>(
            new PeerComparator(targetNodeId)
                .thenComparing(peer -> peer.getKademliaId().toString()));
    Lock lock = peerList.getReadWriteLock().readLock();
    lock.lock();
    try {
      ArrayList<Peer> peerArrayList = peerList.getPeerArrayList();
      if (peerArrayList == null) {
        return null;
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
      log.debug("no candidate peer to forward toward {}", targetNodeId);
      return null;
    }

    // Graph routing needs our own Node, which is wired late during startup — fall back to the
    // greedy Kademlia step below until it is available.
    Node self = serverContext.getNode();
    if (self != null) {
      Node targetNode = serverContext.getNodeStore().get(targetNodeId);
      org.jgrapht.Graph<Node, NodeEdge> graph = serverContext.getNodeStore().getNodeGraph();
      GMParser.RouteSelection selection =
          GMParser.selectBestRoutePeer(
              graph, self, candidates, targetNode, GMParser.MAX_ROUTE_WEIGHT);

      if (selection.peer() != null) {
        return selection.peer();
      }
    }

    // No graph route — greedy Kademlia fallback: next hop must be strictly closer to the target
    // than we are (forward progress). Loop protection is the caller's job: the hop limit for the
    // MS02b OH forwarding, the packet_id dedup for Flaschenpost v2 routing.
    Peer nearest = candidates.first();
    int ourDistance = targetNodeId.getDistance(serverContext.getNonce());
    int nearestDistance = targetNodeId.getDistance(nearest.getKademliaId());
    if (nearestDistance < ourDistance) {
      return nearest;
    }
    log.debug("no peer closer to {} than ourselves, dropping", targetNodeId);
    return null;
  }
}
