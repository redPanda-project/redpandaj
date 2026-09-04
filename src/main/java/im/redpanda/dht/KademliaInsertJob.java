package im.redpanda.dht;

import static com.google.protobuf.ByteString.copyFrom;

import im.redpanda.core.*;
import im.redpanda.ops.Job;
import im.redpanda.proto.KademliaStore;
import im.redpanda.transport.Peer;
import im.redpanda.transport.PeerList;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KademliaInsertJob extends Job {

  private static final Logger logger = LogManager.getLogger();

  public static final int SEND_TO_NODES = 2;
  private static final int NONE = 0;
  private static final int ASKED = 2;
  private static final int SUCCESS = 1;

  private final KadContent kadContent;

  // ConcurrentSkipListMap: ack() is called from network threads while work()
  // iterates this map on a job thread (Sentry REDPANDAJ-2E1)
  private ConcurrentNavigableMap<Peer, Integer> peers = null;

  public KademliaInsertJob(ServerContext serverContext, KadContent kadContent) {
    super(serverContext);
    this.kadContent = kadContent;
  }

  @Override
  public void init() {

    PeerList peerList = serverContext.getPeerList();

    // We first save the KadContent in our StoreManager, we use "dht-caching"
    // such that too far away entries will be removed faster
    serverContext.getKadStoreManager().put(kadContent);

    // lets sort the peers by the destination key
    peers = new ConcurrentSkipListMap<>(new PeerComparator(kadContent.getId()));

    // insert all nodes
    for (Peer p : peerList.snapshot()) {

      if (p.getNodeId() == null) {
        continue;
      }

      peers.put(p, NONE);
    }
  }

  @Override
  public void work() {

    int askedPeers = 0;
    int successfullPeers = 0;
    for (Peer p : peers.keySet()) {

      Integer status = peers.get(p);
      if (status == SUCCESS) {
        successfullPeers++;
        askedPeers++;
        continue;
      } else if (status == ASKED) {
        continue;
      }

      if (successfullPeers >= SEND_TO_NODES) {
        done();
        break;
      }

      if (askedPeers >= SEND_TO_NODES) {
        break;
      }

      if (p.isConnected() && p.isIntegrated()) {

        var storeMsg =
            KademliaStore.newBuilder()
                .setJobId(getJobId())
                .setTimestamp(kadContent.getTimestamp())
                .setPublicKey(copyFrom(kadContent.getPubkey()))
                .setContent(copyFrom(kadContent.getContent()))
                .setSignature(copyFrom(kadContent.getSignature()))
                .build();

        try {
          // lets not wait too long for a lock, since this job may timeout otherwise — a peer
          // whose write buffer stays busy (or that disconnected) is simply not counted as asked.
          if (p.tryEnqueueFrame(
              Command.KADEMLIA_STORE, storeMsg.toByteArray(), 50, TimeUnit.MILLISECONDS)) {
            peers.put(p, ASKED);
            askedPeers++;

            logger.debug(
                "putKadCmd to peer {} (node id {}) size: {} distance: {} target: {}",
                p,
                p.getNodeId(),
                peers.size(),
                kadContent.getId().getDistance(p.getKademliaId()),
                kadContent.getId());
          }
        } catch (InterruptedException e) {
          // Restore the flag: swallowing it left the job's thread looking uninterrupted.
          Thread.currentThread().interrupt();
          logger.debug("interrupted while queueing KADEMLIA_STORE for {}", p);
        }
      }
    }

    // System.out.println("successfullPeers: " + successfullPeers + " askedPeers: "
    // + askedPeers);
    if (successfullPeers >= SEND_TO_NODES) {
      done();
    }
  }

  public void ack(Peer p) {
    // Only peers that have a NodeId ever enter this map (init() skips the others), and the map is
    // sorted by PeerComparator, which dereferences getKademliaId(). Acking a peer without one
    // therefore NPEd inside put() — reachable from the wire: any light client that sends a JOB_ACK
    // carrying a live insert job's id took the whole command loop down with it (found while
    // making the TD133 log line null-safe; the NPE happened before that line was ever reached).
    if (p.getNodeId() == null) {
      logger.debug("ignoring JOB_ACK from {}: peer has no node id, it was never asked", p);
      return;
    }
    peers.put(p, SUCCESS);
  }
}
