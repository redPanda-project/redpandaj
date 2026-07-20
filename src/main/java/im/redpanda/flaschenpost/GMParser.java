package im.redpanda.flaschenpost;

import im.redpanda.core.Command;
import im.redpanda.core.KademliaId;
import im.redpanda.core.Log;
import im.redpanda.core.Node;
import im.redpanda.core.Peer;
import im.redpanda.core.PeerList;
import im.redpanda.core.ServerContext;
import im.redpanda.jobs.Job;
import im.redpanda.jobs.KademliaSearchJob;
import im.redpanda.jobs.PeerPerformanceTestFlaschenpostJob;
import im.redpanda.jobs.PeerPerformanceTestGarlicMessageJob;
import im.redpanda.kademlia.KadContent;
import im.redpanda.kademlia.PeerComparator;
import im.redpanda.kademlia.nodeinfo.GMEntryPointModel;
import im.redpanda.kademlia.nodeinfo.NodeInfoModel;
import im.redpanda.store.NodeEdge;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import lombok.extern.slf4j.Slf4j;
import org.jgrapht.GraphPath;
import org.jgrapht.alg.shortestpath.DijkstraShortestPath;

@Slf4j
public class GMParser {

  /**
   * Upper bound on the total route weight a candidate peer may have to still be selected. Kept at
   * the historical value (20) so single-candidate / well-connected behavior is unchanged.
   */
  static final double MAX_ROUTE_WEIGHT = 20;

  private GMParser() {}

  /**
   * Result of {@link #selectBestRoutePeer}: the chosen candidate peer (or {@code null} if none has
   * a route below the bound) and the total weight of its route.
   */
  record RouteSelection(Peer peer, double weight) {}

  /**
   * Selects the candidate peer that yields the cheapest route to {@code targetNode}.
   *
   * <p>The total cost of routing through a candidate is the weight of the edge {@code self ->
   * candidate.getNode()} (our directly-measured link quality to that neighbour) plus the weight of
   * the shortest path from the candidate's node onward to the target. This is the formulation the
   * weighted {@link NodeEdge} graph is built for ({@code NodeStore} adds self↔peer edges and
   * weights them by link performance), and — unlike the previous code — it actually depends on the
   * candidate being evaluated.
   *
   * <p>The previous implementation computed {@code findPathBetween(self, target)} inside the loop,
   * which is independent of the candidate, so the first peer in {@link TreeSet} order always won
   * and Dijkstra ran |peers| times for nothing.
   *
   * @param graph the weighted node graph
   * @param self the local node (route origin)
   * @param candidates candidate next-hop peers, already filtered to connected non-light-client
   *     peers
   * @param targetNode the destination node (may be {@code null} if unknown to the graph)
   * @param maxWeight inclusive-exclusive upper bound; candidates with total weight {@code >=} this
   *     are ignored
   * @return the best candidate and its total weight; {@code peer()} is {@code null} if none qualify
   */
  static RouteSelection selectBestRoutePeer(
      org.jgrapht.Graph<Node, NodeEdge> graph,
      Node self,
      Iterable<Peer> candidates,
      Node targetNode,
      double maxWeight) {
    double bestWeight = maxWeight;
    Peer bestPeer = null;

    if (targetNode == null) {
      // Destination unknown to the NodeStore — no route can be computed.
      return new RouteSelection(null, maxWeight);
    }

    for (Peer peer : candidates) {
      Node peerNode = peer.getNode();
      if (peerNode == null) {
        continue;
      }

      // Cost of our direct link to this candidate next hop.
      double firstHopWeight;
      NodeEdge selfToPeer =
          graph.containsVertex(self) && graph.containsVertex(peerNode)
              ? graph.getEdge(self, peerNode)
              : null;
      if (selfToPeer == null) {
        // No known direct edge to this candidate — it cannot be our next hop.
        continue;
      }
      firstHopWeight = graph.getEdgeWeight(selfToPeer);

      // Cost of the remaining path from the candidate onward to the target.
      double remainingWeight;
      if (peerNode.equals(targetNode)) {
        remainingWeight = 0;
      } else {
        GraphPath<Node, NodeEdge> path = null;
        try {
          path = DijkstraShortestPath.findPathBetween(graph, peerNode, targetNode);
        } catch (IllegalArgumentException | NullPointerException ignored) {
          // peerNode/targetNode not in graph, or an edge was concurrently removed
          // mid-traversal (Sentry REDPANDAJ-2DW/2E5 pattern) — treat as no route.
        }
        if (path == null) {
          continue;
        }
        remainingWeight = path.getWeight();
      }

      double totalWeight = firstHopWeight + remainingWeight;
      if (totalWeight < bestWeight) {
        bestWeight = totalWeight;
        bestPeer = peer;
      }
    }

    return new RouteSelection(bestPeer, bestPeer == null ? maxWeight : bestWeight);
  }

  public static GMContent parse(ServerContext serverContext, byte[] content) {

    if (content == null || content.length == 0) {
      return null;
    }

    ByteBuffer buffer = ByteBuffer.wrap(content);

    byte type = buffer.get();

    if (type == GMType.GARLIC_MESSAGE.getId()) {

      // v2 (MS03): authenticity is checked by the GCM tag at decryption time — only the
      // recipient can verify it; intermediate nodes just deduplicate and forward.
      GarlicMessage garlicMessage;
      try {
        garlicMessage = new GarlicMessage(serverContext, content);
      } catch (RuntimeException e) {
        // malformed packet from the network (bad version/length/truncated) — drop it instead
        // of letting the exception bubble into the read loop
        Log.put("dropping malformed garlic message: " + e.getMessage(), 50);
        return null;
      }

      boolean alreadyPresent = GMStoreManager.put(garlicMessage);

      if (alreadyPresent) {
        return null;
      }

      garlicMessage.tryParseContent();

      // if the gm is targeted to us the content will be handled by the parseContent
      // routine of the gm
      if (!garlicMessage.isTargetedToUs()) {
        sendGarlicMessageToPeer(serverContext, garlicMessage);
      }

      return garlicMessage;

    } else if (type == GMType.ACK.getId()) {

      // A network-supplied ACK payload may be malformed (wrong length/truncated), or an
      // end-to-end encrypted client payload whose first byte just happens to be the ACK type
      // byte. Dropping it here keeps the malformed packet from unwinding the read loop.
      GMAck gmAck;
      try {
        gmAck = new GMAck(content);
        gmAck.parseContent();
      } catch (RuntimeException e) {
        Log.put("dropping malformed GMAck: " + e.getMessage(), 50);
        return null;
      }

      Job runningJob = Job.getRunningJob(gmAck.getAckid());

      if (runningJob instanceof PeerPerformanceTestFlaschenpostJob perfJob) {
        perfJob.success();
      }

      if (runningJob instanceof PeerPerformanceTestGarlicMessageJob perfJob) {
        perfJob.success();
      }

      return gmAck;
    }

    // Unknown type byte from the network (e.g. an encrypted client payload) — drop it instead
    // of throwing, which would unwind the reader loop and leave the connection retrying.
    Log.put("dropping GM with unknown type: " + type, 50);
    return null;
  }

  /**
   * Cheap structural pre-check: is {@code content} a parseable GM frame (a well-formed {@link
   * GarlicMessage} or {@link GMAck})? Unlike {@link #parse}, this never mutates any state — no
   * dedup-store insert, no job lookups, no forwarding — so it is safe to call purely to decide
   * whether content deserves a BAD_REQUEST response before invoking {@link #parse}.
   *
   * <p>Used by the {@code FlaschenpostPut} legacy (empty {@code oh_id}) path (REDPANDAJ-2DR): a raw
   * E2E-encrypted client payload can accidentally start with a byte that collides with a known
   * {@link GMType} id (e.g. {@code 0x04} == {@link GMType#ACK}), and such content must be rejected
   * explicitly instead of silently swallowed by {@link #parse}'s defensive drop.
   *
   * @return {@code true} if content is a well-formed frame of a known type (regardless of whether
   *     {@link #parse} would then treat it as a duplicate); {@code false} if the type byte is
   *     unknown or the frame is malformed/truncated for its type.
   */
  public static boolean isValidFrame(ServerContext serverContext, byte[] content) {
    if (content == null || content.length == 0) {
      return false;
    }

    byte type = content[0];

    if (type == GMType.GARLIC_MESSAGE.getId()) {
      try {
        new GarlicMessage(serverContext, content);
        return true;
      } catch (RuntimeException e) {
        return false;
      }
    } else if (type == GMType.ACK.getId()) {
      try {
        GMAck gmAck = new GMAck(content);
        gmAck.parseContent();
        return true;
      } catch (RuntimeException e) {
        return false;
      }
    }

    return false;
  }

  private static void sendGarlicMessageToPeer(
      ServerContext serverContext, GarlicMessage garlicMessage) {
    PeerList peerList = serverContext.getPeerList();

    Peer peerToSendFP = peerList.get(garlicMessage.getDestination());

    byte[] content = garlicMessage.getContent();

    if (peerToSendFP == null || !peerToSendFP.isConnected()) {

      Node node = serverContext.getNodeStore().get(garlicMessage.destination);

      if (node != null) {
        KademliaId nodeKademliaId = KadContent.createKademliaId(node.getNodeId());
        KadContent kadContent = serverContext.getKadStoreManager().get(nodeKademliaId);

        if (kadContent == null) {
          log.info(
              "no kademlia content for target peer: "
                  + garlicMessage.destination
                  + " and target kademlia id: "
                  + nodeKademliaId);
          new KademliaSearchJob(serverContext, nodeKademliaId).start();
        } else {
          if (System.currentTimeMillis() - kadContent.getTimestamp()
              > Duration.ofMinutes(8).toMillis()) {
            new KademliaSearchJob(serverContext, nodeKademliaId).start();
          }
          String jsonString = new String(kadContent.getContent());
          NodeInfoModel nodeInfoModel = NodeInfoModel.importFromString(jsonString);
          List<GMEntryPointModel> entryPoints = nodeInfoModel.getEntryPoints();
          Collections.shuffle(entryPoints);

          for (GMEntryPointModel entryPoint : entryPoints) {
            Peer peer = serverContext.getPeerList().get(entryPoint.getNodeId().getKademliaId());
            if (peer == null || !peer.isConnected()) {
              Node.addNodeIfNotPresent(
                  serverContext, entryPoint.getNodeId(), entryPoint.getIp(), entryPoint.getPort());
              continue;
            }
            sendFpToPeer(peer, content);
            return;
          }
        }
      }

      // todo, put all into a job to handle failing peers and retry send if no ack

      TreeSet<Peer> peers = new TreeSet<>(new PeerComparator(garlicMessage.getDestination()));

      // todo use best route for this flaschenpost by network graph

      // insert all nodes
      Lock lock = peerList.getReadWriteLock().readLock();
      lock.lock();
      try {
        ArrayList<Peer> peerArrayList = peerList.getPeerArrayList();

        if (peerArrayList == null) {
          return;
        }

        for (Peer p : peerArrayList) {

          // do not add the peer if the peer is not connected or the nodeId is unknown!
          if (p.getNodeId() == null || !p.isConnected() || !p.hasNode()) {
            continue;
          }

          // do not send fps to light clients
          if (p.isLightClient()) {
            continue;
          }

          // /**
          // * do not add peers which are further or equally away from the key than us
          // */
          // int peersDistanceToKey =
          // garlicMessage.getDestination().getDistance(p.getKademliaId());
          // if (myDistanceToKey <= peersDistanceToKey) {
          // continue;
          // }
          // System.out.println("my distance: " + myDistanceToKey + " theirs distance: " +
          // peersDistanceToKey);

          peers.add(p);
        }
      } finally {
        lock.unlock();
      }

      if (peers.isEmpty()) {
        // System.out.println(String.format("no peer found for destination %s which is
        // near to target", garlicMessage.getDestination()));
        return;
      }

      // maintainNodes() mutates the graph under the NodeStore write lock (Sentry
      // REDPANDAJ-2DW/2E5 fix); take the read lock here so Dijkstra never traverses
      // a graph that is concurrently being restructured.
      Lock nodeStoreLock = serverContext.getNodeStore().getReadWriteLock().readLock();
      RouteSelection selection;
      nodeStoreLock.lock();
      try {
        Node targetNode = serverContext.getNodeStore().get(garlicMessage.destination);
        selection =
            selectBestRoutePeer(
                serverContext.getNodeStore().getNodeGraph(),
                serverContext.getNode(),
                peers,
                targetNode,
                MAX_ROUTE_WEIGHT);
      } finally {
        nodeStoreLock.unlock();
      }
      double shortestPathWeight = selection.weight();
      Peer peerWithShortestPath = selection.peer();

      if (peerWithShortestPath != null) {
        sendFpToPeer(peerWithShortestPath, content);
        int myDistanceToKey = garlicMessage.getDestination().getDistance(serverContext.getNonce());
        KademliaId kademliaId = peerWithShortestPath.getKademliaId();
        int peersDistance = garlicMessage.getDestination().getDistance(kademliaId);
        if (shortestPathWeight > 3) {
          Log.put(
              "inserting fp to peer "
                  + garlicMessage.getDestination()
                  + " since we are not directly connected shortest path "
                  + shortestPathWeight
                  + " "
                  + " distance "
                  + peersDistance
                  + " our distance "
                  + myDistanceToKey
                  + " last "
                  + garlicMessage.getDestination().getDistance(peers.last().getKademliaId())
                  + " node: "
                  + peerWithShortestPath.getNode().getNodeId()
                  + " con "
                  + peerWithShortestPath.isConnected(),
              0);
        }
      }

    } else {
      sendFpToPeer(peerToSendFP, content);
    }
  }

  private static void sendFpToPeer(Peer peerToSendFP, byte[] content) {
    sendFpToPeer(peerToSendFP, content, null, 0, null);
  }

  public static void sendFpToPeer(Peer peerToSendFP, byte[] content, byte[] ohId, int hopCount) {
    sendFpToPeer(peerToSendFP, content, ohId, hopCount, null);
  }

  /**
   * Writes a FlaschenpostPut to the peer. MS02b (Option A): when {@code ohId} is non-null it is
   * preserved on the forwarded packet, so the destination node can deposit into the correct mailbox
   * — before MS02b the packet was rebuilt with {@code content} only and the oh_id was lost. {@code
   * hopCount} carries the forwarding budget (loop protection). {@code sessionTag} (MS05) preserves
   * the reverse-garlic session tag when a tagged deliver is forwarded to the OH host node; {@code
   * null}/empty for untagged deposits.
   */
  public static void sendFpToPeer(
      Peer peerToSendFP, byte[] content, byte[] ohId, int hopCount, byte[] sessionTag) {
    sendFpToPeer(peerToSendFP, content, ohId, hopCount, sessionTag, null);
  }

  /**
   * Writes a FlaschenpostPut that additionally preserves an MS06 return-path block ({@code
   * null}/empty when no R-ACK was requested), so the node making the final deposit decision can
   * send the {@code RoutingAck}.
   */
  public static void sendFpToPeer(
      Peer peerToSendFP,
      byte[] content,
      byte[] ohId,
      int hopCount,
      byte[] sessionTag,
      byte[] returnPath) {
    peerToSendFP.getWriteBufferLock().lock();
    try {
      var builder =
          im.redpanda.proto.FlaschenpostPut.newBuilder()
              .setContent(com.google.protobuf.ByteString.copyFrom(content));
      if (ohId != null) {
        builder.setOhId(com.google.protobuf.ByteString.copyFrom(ohId));
      }
      if (hopCount > 0) {
        builder.setHopCount(hopCount);
      }
      if (sessionTag != null && sessionTag.length > 0) {
        builder.setSessionTag(com.google.protobuf.ByteString.copyFrom(sessionTag));
      }
      if (returnPath != null && returnPath.length > 0) {
        builder.setReturnPath(com.google.protobuf.ByteString.copyFrom(returnPath));
      }
      byte[] data = builder.build().toByteArray();

      peerToSendFP.writeBuffer.put(Command.FLASCHENPOST_PUT);
      peerToSendFP.writeBuffer.putInt(data.length);
      peerToSendFP.writeBuffer.put(data);
      peerToSendFP.setWriteBufferFilled();
    } finally {
      peerToSendFP.getWriteBufferLock().unlock();
    }
  }
}
