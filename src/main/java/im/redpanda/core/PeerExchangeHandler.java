package im.redpanda.core;

import static com.google.protobuf.ByteString.copyFrom;

import com.google.protobuf.InvalidProtocolBufferException;
import im.redpanda.identity.NodeId;
import im.redpanda.identity.crypt.Utils;
import im.redpanda.ops.Log;
import im.redpanda.ops.Settings;
import im.redpanda.proto.NodeIdProto;
import im.redpanda.proto.PeerInfoProto;
import im.redpanda.proto.SendPeerList;
import im.redpanda.routing.graph.Node;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Peer-list gossip and liveness: wire commands 5 (PING), 6 (PONG), 7 (REQUEST_PEERLIST) and 8
 * (SEND_PEERLIST).
 *
 * <p>Split out of {@link InboundCommandProcessor} by T116 (DDD review 2026-08-31, §6 P2 step 2).
 * This one stays in {@code im.redpanda.core}: the peer list <em>is</em> a core concept, and the
 * handlers reach straight into {@link PeerList} and {@link Peer}'s package-private state. The
 * bodies are verbatim; only their visibility changed.
 */
class PeerExchangeHandler {

  private static final Logger logger = LogManager.getLogger();

  private final ServerContext serverContext;

  PeerExchangeHandler(ServerContext serverContext) {
    this.serverContext = serverContext;
  }

  int handlePing(Peer peer) {
    Log.put("Received ping command", 200);
    if (!serverContext.getPeerList().contains(peer.getKademliaId())) {
      logger.error(
          "Got PING from node not in our peerlist, lets add it.... %s, id: %s"
              .formatted(peer, peer.getKademliaId()));
      serverContext.getPeerList().add(peer);
      return 0;
    }
    peer.enqueueCommand(Command.PONG);
    return 1;
  }

  int handlePong(Peer peer) {
    Log.put("Received pong command", 200);
    peer.ping = (1 * peer.ping + (double) (System.currentTimeMillis() - peer.lastPinged)) / 2;
    peer.setLastPongReceived(System.currentTimeMillis());
    return 1;
  }

  int handleRequestPeerList(Peer peer) {
    // T87: PeerList.snapshot() takes the read lock and releases it — the response is built and
    // written WITHOUT it. Holding it across the peer's writeBufferLock inverted the lock order
    // documented on PeerList: ConnectionHandler.setupConnection() holds a peer's writeBufferLock
    // and then takes the peer list WRITE lock, on the NIO selector thread. Reader thread and
    // selector thread could therefore each hold one of the two and wait for the other, and because
    // a ReentrantReadWriteLock is non-fair the selector's queued write request then blocked every
    // subsequent reader as well. That deadlocked a public seed node on 2026-07-29: the selector
    // stopped calling accept(), the listen backlog filled up and no client could connect any more.
    //
    // Snapshotting is also the pattern every other iteration site uses (NodeStore.addServerEdges,
    // PeerJobs.runOnce, Saver.savePeers, OhForwarder.selectNextPeer); the peers themselves were
    // never guarded by this lock, only the list structure, so nothing weakens by copying first.
    List<Peer> snapshot = serverContext.getPeerList().snapshot();

    var builder = SendPeerList.newBuilder();
    for (Peer peerToCheck : snapshot) {
      if (peerToCheck.ip == null) {
        continue;
      }
      // Same predicate as on the ingest path, with the recipient as the "other side": do not
      // hand a local-only address to a peer outside that network, and do not pass on entries
      // that carry no dialable port. Without this we amplify exactly what we refuse to accept.
      if (!Utils.isPlausibleAdvertisedAddress(peerToCheck.ip, peerToCheck.getPort(), peer.ip)) {
        continue;
      }
      var peerBuilder =
          PeerInfoProto.newBuilder().setIp(peerToCheck.ip).setPort(peerToCheck.getPort());
      if (peerToCheck.getNodeId() != null && peerToCheck.getNodeId().hasKey()) {
        peerBuilder.setNodeId(
            NodeIdProto.newBuilder()
                .setPublicKeyBytes(copyFrom(peerToCheck.getNodeId().exportPublic()))
                .build());
        // MS04: explicit X25519 key so light clients can pick garlic hops directly
        peerBuilder.setEncryptionPublicKey(
            copyFrom(peerToCheck.getNodeId().getEncryptionPubKey().getEncoded()));
      }
      builder.addPeers(peerBuilder.build());
    }
    byte[] data = builder.build().toByteArray();
    peer.enqueueFrame(Command.SEND_PEERLIST, data);
    return 1;
  }

  void handleSendPeerList(Peer peer, byte[] bytesForPeerList)
      throws InvalidProtocolBufferException {
    SendPeerList sendPeerList = SendPeerList.parseFrom(bytesForPeerList);
    for (PeerInfoProto peerProto : sendPeerList.getPeersList()) {
      if (serverContext.getPeerList().size() >= Settings.MAX_PEERLIST_SIZE) {
        Log.put("peer list is full, ignoring the rest of the gossiped peer list", 40);
        break;
      }
      NodeId nodeId = null;
      if (peerProto.hasNodeId()) {
        try {
          nodeId = NodeId.importPublic(peerProto.getNodeId().getPublicKeyBytes().toByteArray());
        } catch (IllegalArgumentException e) {
          // malformed or legacy (pre-MS03) key in the peer list — skip this entry
          continue;
        }
      }
      String ip = peerProto.getIp();
      int port = peerProto.getPort();
      // Peer-list gossip is unauthenticated: everything below this point comes from the peer and
      // nothing above verifies it. Reject entries that the advertising peer cannot plausibly know
      // about — otherwise any peer can steer us into dialling loopback, the local LAN or a
      // portless address, and we then spread those entries further.
      if (!Utils.isPlausibleAdvertisedAddress(ip, port, peer.ip)) {
        Log.put("ignoring implausible peer list entry " + ip + ":" + port + " from " + peer.ip, 40);
        continue;
      }
      if (port == serverContext.getPort() && Utils.isOwnHostAddress(ip)) {
        Log.put("ignoring peer list entry that points back at us: " + ip + ":" + port, 40);
        continue;
      }
      if (nodeId != null) {
        if (nodeId.getKademliaId().equals(serverContext.getOwnNodeId())) {
          Log.put("found ourselves in the peerlist", 80);
          continue;
        }
        Peer newPeer = new Peer(ip, port, nodeId);
        var byKademliaId = Node.getByKademliaId(serverContext, nodeId.getKademliaId());
        if (byKademliaId != null) {
          byKademliaId.addConnectionPoint(ip, port);
        } else {
          new Node(serverContext, nodeId);
        }
        serverContext.getPeerList().add(newPeer);
      } else {
        serverContext.getPeerList().add(new Peer(ip, port));
      }
    }
  }
}
