package im.redpanda.core;

import im.redpanda.kademlia.PeerComparator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.jetbrains.annotations.Nullable;

/**
 * This class stores all peers in two Hashmaps for fast get operations using the {@link KademliaId}
 * and Ip+Port. For the connections we establish, we need a sorted List with regard to specific
 * parameters. This class maintains an ArrayList with the same peers as in the Hashmaps. In
 * addition, a peer can be optionally be stored in the DHT routing table, called the Buckets. Note
 * that not all nodes will be in the routing table (Buckets).
 *
 * <p><b>Warning</b>: Do not change any of the required data ({@link KademliaId}, Ip, Port) of a
 * {@link Peer} if it is present in the Peerlist without updating the corresponding List/HashMap.
 *
 * <h2>Lock order (T87)</h2>
 *
 * <p>Three locks are routinely nested on the connection paths. They must always be acquired in this
 * order, outermost first, and never in the reverse one:
 *
 * <ol>
 *   <li>{@link Peer#writeBufferLock} — a single peer's write buffer
 *   <li>{@code NodeStore.readWriteLock} — the node graph
 *   <li>{@code PeerList.readWriteLock} — this lock
 * </ol>
 *
 * <p>Violating it wedged a public seed node on 2026-07-29: {@code
 * ConnectionHandler.setupConnection()} holds a peer's {@code writeBufferLock} and then calls {@link
 * #add(Peer)} (the peer list <i>write</i> lock) on the NIO selector thread, while {@code
 * InboundCommandProcessor.handleRequestPeerList()} held the peer list <i>read</i> lock and then
 * took the same peer's {@code writeBufferLock} on a reader thread. Once the selector's write
 * request was queued, every later reader queued behind it (the lock is non-fair, so a queued writer
 * blocks new readers), so the whole node froze instead of merely slowing down — including {@code
 * accept()}, which is why the listen backlog filled up and no client could connect any more.
 *
 * <p>Only the last of the three is guarded by this class; the other two are documented at their own
 * acquisition sites. The rule for the peer list specifically: <b>never call anything that can block
 * while holding one of these locks.</b> Snapshot the list under the lock, release it, then do the
 * work — see {@code NodeStore.addServerEdges()}, {@code PeerJobs.runOnce()}, {@code
 * Saver.savePeers()}, {@code OhForwarder.selectNextPeer()} for the established pattern.
 */
public class PeerList {

  /**
   * Signals that the peer list write lock could not be acquired within the caller's budget. Only
   * thrown by {@link #add(Peer, long)}; the unbounded {@link #add(Peer)} waits forever.
   */
  public static class PeerListBusyException extends RuntimeException {
    public PeerListBusyException(String message) {
      super(message);
    }
  }

  /** We store each Peer in a hashmap for fast get operations via KademliaId */
  private final HashMap<KademliaId, Peer> peerHashMap = new HashMap<>();

  /** We store each Peer in a hashmap for fast get operations via Ip and Port */
  private final HashMap<Integer, Peer> peerlistIpPort = new HashMap<>();

  /** Blacklist of ips via HashMap */
  private final HashMap<String, Peer> blacklistIp = new HashMap<>();

  /**
   * We store each Peer in a ArrayList to obtain a sorted list of Peers where the good peers are on
   * top
   */
  private final ArrayList<Peer> peerArrayList = new ArrayList<>();

  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

  public PeerList(ServerContext serverContext) {
    initBlacklist();
  }

  /**
   * Adds a Peer to the Peerlist by handling all Hashmaps and the Arraylist. Acquires locks.
   * KademliaId is optional, ip and port have to be known of the Peer.
   *
   * @param peer The peer to add to the PeerList.
   * @return old peer, null if no old peer or old peer null.
   */
  public Peer add(Peer peer) {
    // The duplicate lookups below used to run without any lock while other threads structurally
    // modified the same plain HashMaps under the write lock (remove(), updateKademliaId(),
    // addPeer()). An unsynchronized read during a put/resize/treeify is undefined behaviour:
    // false-negative miss, a torn `oldPeer`, or an NPE inside HashMap.get. add() is called
    // concurrently from the reader threads, the incoming handler and the outbound thread, so the
    // whole check-and-add now runs under the write lock — which also makes it atomic, closing the
    // check-then-add TOCTOU that let two threads both pass the duplicate check for the same peer.
    // The write lock (not a read lock for the lookups) is required precisely because we must not
    // release it between the check and addPeer(), and because ReentrantReadWriteLock cannot
    // upgrade a read lock to a write lock. addPeer() re-acquires it reentrantly.
    readWriteLock.writeLock().lock();
    try {
      return addLocked(peer);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  /**
   * Like {@link #add(Peer)}, but gives up instead of parking forever.
   *
   * <p>For callers that must not block indefinitely — above all {@code
   * ConnectionHandler.setupConnection()}, which runs on the single NIO selector thread. A selector
   * thread parked on a lock stops calling {@code accept()} and stops servicing every existing
   * connection, so a peer list lock that is stuck for any reason takes the entire node down rather
   * than costing one connection (T87). The timeout does not make a stuck lock correct; it turns a
   * silent total wedge into one dropped connection plus a loud Sentry event.
   *
   * @param timeoutMillis how long to wait for the write lock
   * @return old peer, null if no old peer or old peer null — same contract as {@link #add(Peer)}
   * @throws PeerListBusyException if the write lock was not acquired in time, or the wait was
   *     interrupted
   */
  public Peer add(Peer peer, long timeoutMillis) {
    boolean locked;
    try {
      locked = readWriteLock.writeLock().tryLock(timeoutMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PeerListBusyException("interrupted while waiting for the peer list write lock");
    }
    if (!locked) {
      throw new PeerListBusyException(
          "peer list write lock not acquired within " + timeoutMillis + " ms");
    }
    try {
      return addLocked(peer);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  /** The body of {@link #add(Peer)}. Callers must hold the write lock. */
  private Peer addLocked(Peer peer) {
    Peer oldPeer = null;

    // we have to check if the peer is already in the PeerList, for this we use the
    // HashMaps since they are much faster
    if (peer.getKademliaId() != null) {
      oldPeer = peerHashMap.get(peer.getKademliaId());
      if (oldPeer != null) {
        // Peer with same KademliaId exists already
        Log.put("Peer with same KademliaId exists already", 100);
        return oldPeer;
      } else {
        /**
         * Peer has a NodeId but was not found in list. If we would return without checking for ip
         * and port, fast connections to same peer might make a problem.
         */
      }
    }

    /**
     * We allow peers without connection details (ip,port) in the PeerList, since after a wipe of
     * data the new Node has the same (ip,port) but different Identity. The (ip,port) will then be
     * removed from the old Peer. Since we allow Peers without (ip,port) in general we allow to add
     * Peers without (ip,port) here.
     */
    if (peer.getIp() != null) {
      oldPeer = peerlistIpPort.get(getIpPortHash(peer));
      if (oldPeer != null) {
        // Peer with same Ip+Port exists already

        if (peer.getNodeId() == null) {
          // new peer to add has no node id, lets not add it
          return oldPeer;
        }

        if (oldPeer.getNodeId() == null || !oldPeer.getNodeId().equals(peer.getNodeId())) {
        } else {
          return oldPeer;
        }
      }
    }

    return addPeer(peer);
  }

  @Nullable
  private Peer addPeer(Peer peer) {
    Peer oldPeer = null;
    readWriteLock.writeLock().lock();
    try {
      if (peer.getKademliaId() != null) {
        oldPeer = peerHashMap.put(peer.getKademliaId(), peer);
      }
      peerlistIpPort.put(getIpPortHash(peer), peer);
      peerArrayList.add(peer);
    } finally {
      readWriteLock.writeLock().unlock();
    }
    return oldPeer;
  }

  /**
   * Hash method for the peerlistIpPort list.
   *
   * @param peer
   * @return hash value
   */
  private Integer getIpPortHash(Peer peer) {
    return getIpPortHash(peer.getIp(), peer.getPort());
  }

  private static Integer getIpPortHash(String ip, int port) {
    // ToDo: we need later a better method
    return ip.hashCode() + port;
  }

  /**
   * Removes a {@link Peer} from the PeerList. Removes the Peer from both Hashmaps and the ArrayList
   *
   * @param peer
   */
  public boolean remove(Peer peer) {
    readWriteLock.writeLock().lock();
    try {
      if (peer.getKademliaId() == null) {
        return removeByObject(peer);
      }
      return remove(peer.getKademliaId());
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  private boolean removeByObject(Peer peer) {
    readWriteLock.writeLock().lock();
    try {
      boolean removed = peerArrayList.remove(peer);
      if (!removed) {
        return false;
      }
      if (peer.getIp() != null && peer.getPort() != 0) {
        peerlistIpPort.remove(getIpPortHash(peer));
      }
      return true;
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  /**
   * Completely removes the Peer from all Lists by Ip and Port.
   *
   * @param ip
   * @param port
   * @return
   */
  public boolean removeIpPort(String ip, int port) {
    readWriteLock.writeLock().lock();
    try {
      Peer peer = peerlistIpPort.remove(getIpPortHash(ip, port));
      if (peer == null) {
        return false;
      }
      peerHashMap.remove(peer.getKademliaId());
      peerArrayList.remove(peer);
      return true;
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  /**
   * Removes the Peer from the IpPortList, peer is still in the other lists. Use this only for
   * ip,port changes.
   *
   * @param ip
   * @param port
   * @return
   */
  public boolean removeIpPortOnly(String ip, int port) {
    readWriteLock.writeLock().lock();
    try {
      Peer peer = peerlistIpPort.remove(getIpPortHash(ip, port));
      return peer != null;
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  /**
   * Removes a {@link Peer} by providing a {@link KademliaId} from the PeerList. Removes the Peer
   * from both Hashmaps and the ArrayList
   *
   * @param id
   */
  public boolean remove(KademliaId id) {
    boolean removedOnePeer = false;
    readWriteLock.writeLock().lock();
    try {
      Peer remove = peerHashMap.remove(id);
      if (remove == null) {
        return false;
      }
      removedOnePeer = peerArrayList.remove(remove);
      if (remove.getIp() != null && remove.getPort() != 0) {
        peerlistIpPort.remove(getIpPortHash(remove));
      }
    } finally {
      readWriteLock.writeLock().unlock();
    }
    return removedOnePeer;
  }

  /** clears all underlying lists and Hashmaps. Does not acquire locks. */
  public void clear() {
    peerHashMap.clear();
    peerArrayList.clear();
    peerlistIpPort.clear();
  }

  public boolean contains(KademliaId id) {
    // lock() outside the try: inside it, a throwing lock() would send the finally into an unlock()
    // of a lock this thread never took.
    readWriteLock.readLock().lock();
    try {
      return peerHashMap.containsKey(id);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public Peer get(KademliaId id) {
    readWriteLock.readLock().lock();
    try {
      return peerHashMap.get(id);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public ReadWriteLock getReadWriteLock() {
    return readWriteLock;
  }

  public ArrayList<Peer> getPeerArrayList() {
    return peerArrayList;
  }

  /**
   * Returns the size of the ArrayList which should contain all Peers.
   *
   * @return
   */
  public int size() {
    readWriteLock.readLock().lock();
    try {
      return peerArrayList.size();
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  /**
   * Call this method to update an identity/KademliaId of a Peer.
   *
   * @param peer
   */
  public void updateKademliaId(Peer peer, KademliaId newId) {

    KademliaId oldId = peer.getKademliaId();
    System.out.println("updating KadId, old " + oldId + " new: " + newId.toString());

    readWriteLock.writeLock().lock();
    try {
      if (oldId != null) {
        // We have to remove the old id
        peerHashMap.remove(oldId);
      }
      peer.setNodeId(new NodeId(newId));
      peerHashMap.put(newId, peer);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public Peer getGoodPeer() {
    return getGoodPeer(0.4f);
  }

  public Peer getGoodPeer(float upperPercent) {
    readWriteLock.writeLock().lock();
    try {
      Collections.sort(peerArrayList);

      int size = peerArrayList.size();

      if (size == 0) {
        return null;
      }

      // lets get a random x percent peer
      int max = (int) Math.ceil(size * upperPercent);

      int i = Server.secureRandom.nextInt(max);

      return peerArrayList.get(i);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public void clearConnectionDetails(Peer peer) {
    Log.put("clearing peer: " + peer.getIp() + ":" + peer.getPort(), 50);
    removeIpPortOnly(peer.getIp(), peer.getPort());
    peer.removeIpAndPort();
  }

  /**
   * does not use connected peers or light clients
   *
   * @param targetId
   * @return
   */
  public Peer getClosestGoodPeer(KademliaId targetId) {

    Peer goodPeer = get(targetId);
    if (goodPeer == null || !goodPeer.isConnected()) {

      TreeSet<Peer> peers = new TreeSet<>(new PeerComparator(targetId));

      // insert all nodes
      Lock lock = getReadWriteLock().readLock();
      lock.lock();
      try {
        ArrayList<Peer> peerArrayList = getPeerArrayList();

        for (Peer peer : peerArrayList) {

          // do not add the peer if the peer is not connected or the nodeId is unknown!
          if (peer.getNodeId() == null || !peer.isConnected()) {
            continue;
          }

          // remove all light clients
          if (peer.isLightClient()) {
            continue;
          }

          if (peer.getNode() == null) {
            continue;
          }

          // if (targetId.getDistanceToUs(serverContext) <
          // peer.getKademliaId().getDistance(targetId)) {
          // continue;
          // }

          peers.add(peer);
        }
      } finally {
        lock.unlock();
      }

      if (peers.size() == 0) {
        // System.out.println(String.format("no peer found for destination %s which is
        // near to target", targetId));
        return null;
      }

      goodPeer = peers.first();
    }
    return goodPeer;
  }

  private void initBlacklist() {
    for (String ip : Settings.blacklistIps) {
      blacklistIp.put(ip, null);
    }
  }

  public boolean isBlacklisted(String ip) {
    return blacklistIp.containsKey(ip);
  }
}
