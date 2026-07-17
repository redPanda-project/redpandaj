package im.redpanda.flaschenpost;

import im.redpanda.core.KademliaId;
import im.redpanda.core.Node;
import im.redpanda.core.Peer;
import im.redpanda.core.PeerList;
import im.redpanda.core.ServerContext;
import im.redpanda.jobs.Job;
import im.redpanda.jobs.OhResolveJob;
import im.redpanda.kademlia.PeerComparator;
import im.redpanda.outbound.OutboundService;
import im.redpanda.store.NodeEdge;
import java.util.ArrayList;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
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

  /**
   * Delay before the single retry of a deposit whose first host resolution failed. Chosen to cover
   * the observed race window: a light client whose OH registration was cut off re-registers after
   * its 10 s response timeout, so by retry time the OH is registered (and announced) again.
   */
  static final long RETRY_DELAY_MS = 15_000;

  /**
   * Cap on concurrently parked deposits awaiting their retry. Each payload is at most {@link
   * im.redpanda.outbound.OutboundMailboxStore#MAX_ITEM_BYTES} (pre-checked by the caller), so the
   * buffer is memory-bounded at ~4 MiB; the single fixed-delay retry doubles as the TTL.
   */
  static final int MAX_PENDING_RETRIES = 64;

  /** Number of currently parked deposits. Package-private for tests. */
  static final AtomicInteger pendingRetries = new AtomicInteger();

  /**
   * Everything needed to re-attempt a deposit after a failed host resolution. {@code returnPath}
   * keeps the raw wire bytes (forwarded verbatim), {@code ackPath} the parsed copy that decides
   * whether a drop can R-ACK (both may be {@code null}).
   */
  record PendingDeposit(
      byte[] ohId,
      byte[] content,
      int hopCount,
      byte[] sessionTag,
      byte[] returnPath,
      ReturnPath ackPath) {}

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
    PendingDeposit deposit =
        new PendingDeposit(ohId, content, hopCount, sessionTag, returnPath, ackPath);
    OhResolveJob.resolve(
        serverContext,
        ohId,
        record ->
            routeToNode(serverContext, new KademliaId(record.getNodeId().toByteArray()), deposit),
        () -> onResolveFailed(serverContext, deposit, false));
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
   * Async resolve-failure handling. A first failure does NOT drop: the announce record of a freshly
   * (re-)registered OH may simply not exist yet — the observed failure mode is a light client whose
   * registration was cut off mid-connection, re-registering ~10 s later while the sender's deposit
   * already arrived (T32). The deposit is parked for a single delayed retry instead (bounded
   * buffer, see {@link #MAX_PENDING_RETRIES}).
   *
   * <p>Only the retry's failure ({@code finalAttempt}) or a full buffer drops for real. The sender
   * was already answered OK by the caller, so the drop sends exactly one {@code HANDLE_EXPIRED} ack
   * when the deposit carried a valid return path (see the double-ack invariant in {@link
   * #forward(ServerContext, byte[], byte[], int, byte[], byte[])}); otherwise a plain silent drop.
   */
  static void onResolveFailed(
      ServerContext serverContext, PendingDeposit deposit, boolean finalAttempt) {
    if (!finalAttempt && parkForRetry(serverContext, deposit)) {
      return;
    }
    log.debug("OH host resolution failed, dropping forwarded deposit");
    if (deposit.ackPath() != null) {
      RoutingAckSender.send(
          serverContext, deposit.ackPath(), RoutingAckSender.STATUS_HANDLE_EXPIRED);
    }
  }

  /**
   * Parks the deposit for one delayed retry. Returns {@code false} when the bounded buffer is full
   * — the caller then drops immediately (pre-retry behavior).
   */
  private static boolean parkForRetry(ServerContext serverContext, PendingDeposit deposit) {
    if (pendingRetries.incrementAndGet() > MAX_PENDING_RETRIES) {
      pendingRetries.decrementAndGet();
      log.debug("pending-deposit buffer full, dropping without retry");
      return false;
    }
    try {
      new RetryDepositJob(serverContext, deposit).start();
    } catch (RuntimeException e) {
      // scheduling failed (e.g. executor shutting down) — undo the reservation so the buffer
      // cannot leak full, and let the caller fall back to the immediate final drop
      pendingRetries.decrementAndGet();
      log.debug("failed to schedule deposit retry, dropping: {}", e.getMessage());
      return false;
    }
    return true;
  }

  /**
   * The delayed retry: tries the local deposit first — the observed race is the OH (re-)registering
   * on THIS node right after the failed resolve, which needs no announce record at all — then
   * resolves again. The second resolve failure is final.
   */
  static void retryDeposit(ServerContext serverContext, PendingDeposit deposit) {
    if (depositLocally(serverContext, deposit)) {
      return;
    }
    OhResolveJob.resolve(
        serverContext,
        deposit.ohId(),
        record ->
            routeToNode(serverContext, new KademliaId(record.getNodeId().toByteArray()), deposit),
        () -> onResolveFailed(serverContext, deposit, true));
  }

  /**
   * Attempts the local deposit for a parked or self-resolved packet. Returns {@code true} when the
   * deposit was decided here (stored or terminally rejected, with the matching R-ACK when a return
   * path was carried), {@code false} when the OH is unknown locally and the caller should keep
   * resolving/routing.
   */
  private static boolean depositLocally(ServerContext serverContext, PendingDeposit deposit) {
    OutboundService outboundService = serverContext.getOutboundService();
    if (outboundService == null) {
      return false;
    }
    OutboundService.DepositResult result =
        outboundService.depositMessage(deposit.ohId(), deposit.content(), deposit.sessionTag());
    if (result == OutboundService.DepositResult.NOT_FOUND) {
      return false;
    }
    if (deposit.ackPath() != null) {
      RoutingAckSender.send(serverContext, deposit.ackPath(), RoutingAckSender.statusFor(result));
    }
    return true;
  }

  /**
   * Routes the packet toward {@code targetNodeId}: deposited locally if the record points at this
   * node itself (the OH registered here between the local NOT_FOUND check and the resolve
   * callback), directly if connected, otherwise via the cheapest next hop in the weighted node
   * graph (same selection as garlic routing).
   */
  static void routeToNode(
      ServerContext serverContext, KademliaId targetNodeId, PendingDeposit deposit) {
    KademliaId self = serverContext.getNonce();
    if (self != null && targetNodeId.equals(self)) {
      if (!depositLocally(serverContext, deposit) && deposit.ackPath() != null) {
        // announce points at us but the OH is not registered here (expired/revoked) — final drop
        RoutingAckSender.send(
            serverContext, deposit.ackPath(), RoutingAckSender.STATUS_HANDLE_EXPIRED);
      }
      return;
    }
    Peer nextPeer = selectNextPeer(serverContext, targetNodeId);
    if (nextPeer != null) {
      GMParser.sendFpToPeer(
          nextPeer,
          deposit.content(),
          deposit.ohId(),
          deposit.hopCount() + 1,
          deposit.sessionTag(),
          deposit.returnPath());
      return;
    }
    // Resolved the host node but found no usable next hop: another async drop after the caller
    // already answered OK. Ack HANDLE_EXPIRED (best available status) so the sender does not wait
    // out its timeout. This runs only on the resolve-SUCCESS branch, mutually exclusive with the
    // resolve-failure ack in onResolveFailed, so still exactly one ack per packet.
    if (deposit.ackPath() != null) {
      RoutingAckSender.send(
          serverContext, deposit.ackPath(), RoutingAckSender.STATUS_HANDLE_EXPIRED);
    }
  }

  /** One-shot job holding a parked deposit until its single delayed retry fires. */
  static class RetryDepositJob extends Job {

    private final PendingDeposit deposit;

    RetryDepositJob(ServerContext serverContext, PendingDeposit deposit) {
      // skipImminentRun: only the delayed run does work, mirroring OhResolveJob.DelayedSearchJob
      super(serverContext, RETRY_DELAY_MS, false, true);
      this.deposit = deposit;
    }

    @Override
    public void init() {
      // no setup needed
    }

    @Override
    public void work() {
      try {
        pendingRetries.decrementAndGet();
        retryDeposit(serverContext, deposit);
      } finally {
        done();
      }
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
