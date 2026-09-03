package im.redpanda.transport;

import im.redpanda.core.Server;
import im.redpanda.identity.KademliaId;
import im.redpanda.identity.NodeId;
import im.redpanda.ops.Log;
import im.redpanda.ops.Settings;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
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
 * Saver.savePeers()}, {@code OhForwarder.selectNextPeer()}, {@code OutboundHandler.run()} for the
 * established pattern.
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

  /**
   * We store each Peer in a hashmap for fast get operations via Ip and Port.
   *
   * <p>The key is the address itself ({@code ip + ":" + port}, see {@link #ipPortKey}), not a hash
   * of it. It used to be {@code ip.hashCode() + port}, i.e. an {@code int} that two
   * <em>different</em> addresses can share: {@code "10.0.0.11":59558} and {@code "10.0.0.21":59527}
   * collide, and so does every pair whose ip hashes differ by exactly the port difference. A
   * colliding peer took over the slot of a live one, which made {@link #addLocked} answer with the
   * wrong peer (and, for a peer without a {@link NodeId}, refuse to register the new one at all)
   * and let {@link #removeIpPort} cascade a full removal onto an innocent peer at a completely
   * different address (TD027).
   *
   * <p>Only peers that have an ip are in here — see {@link #addPeer}. Peers without connection
   * details are explicitly allowed in the peer list, and there is no address to key them by.
   *
   * <p>Note what this map still cannot distinguish, because it is not a collision but genuine key
   * equality: every inbound light client from the same ip announces port 0 (it has no listening
   * socket, {@code ConnectionReaderThread:151}), so they all share the key {@code "127.0.0.1:0"}
   * and the last one to be added owns it. That is a lookup ambiguity, not corruption — every
   * mutation of this map is value-checked ({@link #removeIpPortMapping}), so a removal can only
   * ever drop the mapping it actually owns.
   */
  private final HashMap<String, Peer> peerlistIpPort = new HashMap<>();

  /** Blacklist of ips via HashMap */
  private final HashMap<String, Peer> blacklistIp = new HashMap<>();

  /**
   * We store each Peer in a ArrayList to obtain a sorted list of Peers where the good peers are on
   * top
   */
  private final ArrayList<Peer> peerArrayList = new ArrayList<>();

  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

  public PeerList() {
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
      oldPeer = peerlistIpPort.get(ipPortKey(peer));
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
      // Only keyable peers go into the address map. A peer without an ip has no address, and
      // addLocked's javadoc explicitly allows such peers in the list (clearConnectionDetails
      // produces them). Keying them anyway would put every one of them into one shared bucket.
      if (peer.getIp() != null) {
        peerlistIpPort.put(ipPortKey(peer), peer);
      }
      peerArrayList.add(peer);
    } finally {
      readWriteLock.writeLock().unlock();
    }
    return oldPeer;
  }

  /**
   * Key method for the {@link #peerlistIpPort} map.
   *
   * @param peer a peer with an ip
   * @return the address key of that peer
   */
  private static String ipPortKey(Peer peer) {
    return ipPortKey(peer.getIp(), peer.getPort());
  }

  /**
   * The address itself, so that two different addresses can never share a key.
   *
   * <p>Was {@code ip.hashCode() + port} — a hash used as if it were a key. Distinct addresses whose
   * ip hashes differ by exactly the port difference mapped to the same slot ({@code
   * "10.0.0.11":59558} vs {@code "10.0.0.21":59527}), and since peers announce their own ip and
   * port in the gossiped peer list, the colliding entry was remote-controllable (TD027).
   */
  private static String ipPortKey(String ip, int port) {
    return ip + ":" + port;
  }

  /**
   * Drops this peer's {@link #peerlistIpPort} entry, but only while that entry still points at this
   * very peer.
   *
   * <p>{@link #addPeer} inserts <em>every</em> peer into that map, so every removal has to take it
   * out again. The two removal paths used to skip it for {@code port == 0}, which desynchronised
   * the maps for exactly the peers an inbound light client produces: the handshake carries the
   * sender's listening port and a light client has none, so it announces 0 ({@code
   * ConnectionReaderThread:151}). The entry then survived in {@code peerlistIpPort} while the
   * {@link KademliaId} key was gone, and {@link #addLocked}'s ip+port branch handed that ghost back
   * as {@code oldPeer} on the next connection of the same identity — so the reconnecting peer was
   * never registered and {@code ConnectionHandler.setupConnection} dropped it as a TD020 duplicate,
   * on every retry, forever (T88; the S4 airplane-mode gate hung on it after T86/#294 started
   * evicting undialable peers and thus made this removal path reachable at all).
   *
   * <p>The value-checked removal matters because an address is not unique per peer: every inbound
   * light client from the same ip announces port 0, so they all share the key {@code "127.0.0.1:0"}
   * and only the last one added owns it. An unconditional {@code remove(key)} would evict a
   * different, live peer's mapping. This is the single mutation point for removals from {@link
   * #peerlistIpPort}, so that invariant holds for every path (TD027).
   */
  private boolean removeIpPortMapping(Peer peer) {
    if (peer.getIp() == null) {
      return false;
    }
    return peerlistIpPort.remove(ipPortKey(peer), peer);
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

  /**
   * Removes exactly this {@link Peer} object from all three indices, leaving every other peer
   * alone.
   *
   * <p>Needed because {@link #remove(Peer)} removes <em>whoever currently owns the peer's
   * KademliaId</em>, which is not necessarily the object handed in: the list can hold two {@code
   * Peer} objects for the same node (an id-less seed/restored entry plus the one built from a
   * handshake, {@link #addLocked}'s ip+port branch; or a {@link #updateKademliaId} that moved the
   * id onto a second object). Removing the "duplicate" by id then evicts the registered, live peer
   * and leaves the duplicate behind — the exact inversion this method avoids (TD142).
   *
   * <p>Both map removals are value-checked, so an entry that points at a different peer survives.
   *
   * @param peer the object to drop
   * @return true if this very object was in the list
   */
  public boolean removeExact(Peer peer) {
    readWriteLock.writeLock().lock();
    try {
      // ArrayList.remove uses equals(), which Peer does not override -- identity, as intended.
      boolean removed = peerArrayList.remove(peer);
      if (peer.getKademliaId() != null) {
        peerHashMap.remove(peer.getKademliaId(), peer);
      }
      removeIpPortMapping(peer);
      return removed;
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
      removeIpPortMapping(peer);
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
    if (ip == null) {
      return false;
    }
    readWriteLock.writeLock().lock();
    try {
      Peer peer = peerlistIpPort.remove(ipPortKey(ip, port));
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
   * Removes this peer from the IpPortList, the peer is still in the other lists. Use this only for
   * ip,port changes.
   *
   * <p>Takes the {@link Peer} rather than an ip and a port so that the removal can be
   * value-checked: an address does not identify a peer (see {@link #peerlistIpPort}), so removing
   * by address alone evicted whichever peer happened to own that key — the very mistake the other
   * two removal paths were fixed for in T88, left behind on this one (TD027). Its caller {@link
   * #clearConnectionDetails} always has the peer.
   *
   * @param peer the peer whose address mapping should go
   * @return true if this peer's own mapping was removed, false if it did not own one
   */
  public boolean removeIpPortOnly(Peer peer) {
    readWriteLock.writeLock().lock();
    try {
      return removeIpPortMapping(peer);
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
      removeIpPortMapping(remove);
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

  /**
   * The lock guarding the three indices.
   *
   * <p>Package-private since T115: production code no longer needs it. Every caller either used it
   * to take a snapshot — that is {@link #snapshot()} now — or to sort the list, which is {@link
   * #sortByPriority()}. Tests in this package still use it to assert that a code path really does
   * take the lock (see {@code ConcurrencyTestSupport}).
   */
  ReadWriteLock getReadWriteLock() {
    return readWriteLock;
  }

  /**
   * A copy of the peer list, taken under the read lock.
   *
   * <p>This is the only way out of this class for the whole list, and it replaces {@code
   * getPeerArrayList()}, which handed out the live {@link ArrayList} and left every caller to
   * implement the locking itself (T115; the DDD review calls this out as the leak that makes {@code
   * PeerList} an aggregate in name only).
   *
   * <p>Copy, not a locked iteration: iterating under the lock is what wedged a public seed node on
   * 2026-07-29 (T87). The loops around these snapshots connect sockets, disconnect peers and sleep
   * per peer, and all of that takes a peer's {@code writeBufferLock} — the outermost lock in the
   * documented order, which the selector thread holds while it waits for this list's write lock.
   * The copy keeps the iteration {@code ConcurrentModificationException}-safe exactly as the held
   * lock did; nothing outside this class ever needed the lock for anything else, since it guards
   * the list structure and never the {@link Peer} objects in it.
   *
   * <p>Iteration order is the list's own order, i.e. whatever {@link #sortByPriority()} last
   * produced — unchanged by this method.
   */
  public List<Peer> snapshot() {
    readWriteLock.readLock().lock();
    try {
      return new ArrayList<>(peerArrayList);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  /**
   * Sorts the peer list so that the good peers are on top ({@link Peer#compareTo(Peer)}).
   *
   * <p>Under the write lock: the sort mutates the list in place. Callers used to take {@code
   * getReadWriteLock().writeLock()} and call {@code Collections.sort()} on the live list
   * themselves.
   *
   * @throws IllegalArgumentException if the comparison contract is violated mid-sort — a peer's
   *     priority depends on mutable state ({@code connected}, {@code retries}, the node's test
   *     counters), so a concurrent change can make {@code TimSort} throw. {@code OutboundHandler}
   *     handles this by skipping the round; it is deliberately not swallowed here.
   */
  public void sortByPriority() {
    readWriteLock.writeLock().lock();
    try {
      Collections.sort(peerArrayList);
    } finally {
      readWriteLock.writeLock().unlock();
    }
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
    removeIpPortOnly(peer);
    peer.removeIpAndPort();
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
