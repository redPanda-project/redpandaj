package im.redpanda.dht;

import static com.google.protobuf.ByteString.copyFrom;

import im.redpanda.core.Command;
import im.redpanda.core.KademliaId;
import im.redpanda.core.Peer;
import im.redpanda.core.PeerList;
import im.redpanda.core.ServerContext;
import im.redpanda.jobs.Job;
import im.redpanda.proto.KademliaGet;
import im.redpanda.proto.KademliaIdProto;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class KademliaSearchJob extends Job {

  /**
   * Here we use a blacklist to block search request for the same KademliaId in short time
   * intervals. If a search is initialized from a node and the next nodes also have to start a
   * search request, it is possible that two nodes request the same search from us. This blacklist
   * will block any duplicated request for the same search.
   */
  private static final HashMap<KademliaId, Long> kademliaIdSearchBlacklist =
      new HashMap<KademliaId, Long>();

  private static final ReentrantLock kademliaIdSearchBlacklistLock = new ReentrantLock();

  /**
   * How long a KademliaId stays blacklisted. Expired entries are evicted by {@link
   * KademliaSearchJobHousekeeper}, which is started in {@code App}.
   */
  static final long BLACKLIST_KEY_FOR = 1000L * 30L;

  public static final int SEND_TO_NODES = 2;
  private static final int NONE = 0;
  private static final int ASKED = 2;
  private static final int SUCCESS = 1;

  private final KademliaId id;

  // ConcurrentSkipListMap: ack() is called from network threads while work()
  // iterates this map on a job thread (Sentry REDPANDAJ-2E9)
  private ConcurrentNavigableMap<Peer, Integer> peers = null;
  private final ArrayList<KadContent> contents = new ArrayList<>();

  public KademliaSearchJob(ServerContext serverContext, KademliaId id) {
    super(serverContext);
    this.id = id;
  }

  @Override
  public void init() {

    /*
     * Check whether this KademliaId was searched recently, so that no search loops occur. After a
     * search the key stays blacklisted for BLACKLIST_KEY_FOR; while it is, only direct searches are
     * answered for that key. TODO: maybe we should keep a list of "requesters" per search so that
     * we can send an answer to all of them.
     */
    long currentTimeMillis = System.currentTimeMillis();

    kademliaIdSearchBlacklistLock.lock();
    try {
      Long blacklistedTill = kademliaIdSearchBlacklist.get(id);
      if (blacklistedTill == null || currentTimeMillis - blacklistedTill >= 0) {
        kademliaIdSearchBlacklist.put(id, currentTimeMillis + BLACKLIST_KEY_FOR);
      } else {
        // todo: maybe we should inform the peer that he should retry a KadSearch in
        // some seconds?
        fail();
        done();
        return;
      }
    } finally {
      kademliaIdSearchBlacklistLock.unlock();
    }

    // key is not blacklisted, lets sort the peers by the destination key
    peers = new ConcurrentSkipListMap<>(new PeerComparator(id));

    PeerList peerList = serverContext.getPeerList();

    // insert all nodes
    for (Peer p : peerList.snapshot()) {

      // do not add the peer if the peer is not connected or the nodeId is unknown!
      if (p.getNodeId() == null || !p.isConnected()) {
        continue;
      }

      // do not ask light clients for kad entries...
      if (p.isLightClient()) {
        continue;
      }

      peers.put(p, NONE);
    }
  }

  @Override
  public void work() {
    // init() returns early (blacklisted id: fail() + done(), see above) before peers is
    // set. done() cancels the recurring future, but a concurrently already-dispatched
    // run() can still reach work() in that same window (Sentry REDPANDAJ-2E3) — bail out
    // instead of NPEing on peers.keySet().
    if (peers == null) {
      return;
    }

    /** check for timeout, maybe we already got an answer but not SEND_TO_NODES */
    if (getEstimatedRuntime() > 1000 * 5) {
      System.out.println("5 second timeout reached for KadSearch... ");
      success();
      done();
      return;
    }

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
        break;
      }

      if (askedPeers >= SEND_TO_NODES) {
        // the check for done will be made below the loop
        break;
      }

      if (p.isConnected() && p.isIntegrated()) {

        var getMsg =
            KademliaGet.newBuilder()
                .setJobId(getJobId())
                .setSearchedId(
                    KademliaIdProto.newBuilder().setKeyBytes(copyFrom(id.getBytes())).build())
                .build();

        try {
          // lets not wait too long for a lock, since this job may timeout otherwise — a peer
          // whose write buffer stays busy (or that disconnected) is simply not counted as asked.
          if (p.tryEnqueueFrame(
              Command.KADEMLIA_GET, getMsg.toByteArray(), 50, TimeUnit.MILLISECONDS)) {
            peers.put(p, ASKED);
            askedPeers++;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    /**
     * Lets check if already SEND_TO_NODES peers answered and check if all peers list answered, the
     * peers list may be small if we are near the search key...
     */
    if (successfullPeers >= SEND_TO_NODES || successfullPeers == peers.size()) {
      success();
      done();
    }
  }

  protected void fail() {}

  protected ArrayList<KadContent> success() {

    synchronized (contents) {
      if (contents.isEmpty()) {
        return null;
      }

      // lets get the newest one!
      contents.sort(
          (o1, o2) ->
              o1.getTimestamp() < o2.getTimestamp()
                  ? -1
                  : o1.getTimestamp() > o2.getTimestamp() ? 1 : 0);

      return contents;
    }
  }

  public void ack(KadContent c, Peer p) {
    synchronized (contents) {
      contents.add(c);
    }
    peers.put(p, SUCCESS);
  }

  public static HashMap<KademliaId, Long> getKademliaIdSearchBlacklist() {
    return kademliaIdSearchBlacklist;
  }

  public static ReentrantLock getKademliaIdSearchBlacklistLock() {
    return kademliaIdSearchBlacklistLock;
  }
}
