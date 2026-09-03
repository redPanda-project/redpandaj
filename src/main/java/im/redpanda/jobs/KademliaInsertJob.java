package im.redpanda.jobs;

import static com.google.protobuf.ByteString.copyFrom;

import im.redpanda.core.*;
import im.redpanda.kademlia.KadContent;
import im.redpanda.kademlia.PeerComparator;
import im.redpanda.proto.KademliaStore;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

public class KademliaInsertJob extends Job {

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

            System.out.println(
                "putKadCmd to peer: "
                    + p.getNodeId().toString()
                    + " size: "
                    + peers.size()
                    + " distance: "
                    + kadContent.getId().getDistance(p.getKademliaId())
                    + " target: "
                    + kadContent.getId());
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
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
    // todo: concurrency?
    peers.put(p, SUCCESS);
  }
}
