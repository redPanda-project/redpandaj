package im.redpanda.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class PeerListTest {

  @Test
  void add() throws Exception {

    ServerContext serverContext = ServerContext.buildDefaultServerContext();

    PeerList peerList = serverContext.getPeerList();

    Peer mtestip = new Peer("mtestip", 5);

    boolean b = peerList.getReadWriteLock().writeLock().tryLock(5, TimeUnit.SECONDS);

    if (!b) {
      ThreadInfo[] threads = ManagementFactory.getThreadMXBean().dumpAllThreads(true, true);
      for (ThreadInfo info : threads) {
        System.out.print(info);
      }
      System.out.println("lock not possible for add test");
      return;
    }

    int initSize = peerList.size();
    peerList.add(mtestip);

    assertEquals(1, peerList.size() - initSize);
    peerList.add(mtestip);
    assertEquals(1, peerList.size() - initSize);

    Peer mtestipWithNodeId = new Peer("mtestip", 5);
    mtestipWithNodeId.setNodeId(new NodeId());
    peerList.add(mtestipWithNodeId);
    assertEquals(2, peerList.size() - initSize);

    peerList.getReadWriteLock().writeLock().unlock();
  }

  @Test
  void addWithSameKadId() throws Exception {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();

    PeerList peerList = serverContext.getPeerList();

    // different Ips but same KadId
    KademliaId kademliaId = new KademliaId();
    NodeId nodeId = new NodeId(kademliaId);

    Peer peerWithKadId1 = new Peer("mtestip1", 5, nodeId);
    Peer peerWithKadId2 = new Peer("mtestip2", 6, nodeId);

    int initSize = peerList.size();
    peerList.add(peerWithKadId1);

    assertEquals(1, peerList.size() - initSize);
    peerList.add(peerWithKadId2);
    assertEquals(1, peerList.size() - initSize);
  }

  @Test
  void remove() {

    ServerContext serverContext = ServerContext.buildDefaultServerContext();

    PeerList peerList = serverContext.getPeerList();

    Peer toRemovePeerIp = new Peer("toRemovePeerIp", 5);

    int initSize = peerList.size();
    peerList.add(toRemovePeerIp);

    assertEquals(1, peerList.size() - initSize);

    peerList.remove(toRemovePeerIp);

    assertEquals(0, peerList.size() - initSize);
  }

  @Test
  void removeByKademliaId() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();

    PeerList peerList = serverContext.getPeerList();
    Peer peer = new Peer("127.0.0.2", 50558);
    NodeId id = new NodeId();
    peer.setNodeId(id);
    peerList.add(peer);

    Peer peer2 = new Peer("127.0.0.1", 50558);
    NodeId id2 = new NodeId();
    peer.setNodeId(id2);
    peerList.add(peer2);

    peerList.remove(id.getKademliaId());

    assertEquals(1, peerList.size());
    assertNotEquals(peer, peerList.getGoodPeer());
  }

  @Test
  void removeIpPort() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();

    PeerList peerList = serverContext.getPeerList();
    peerList.add(new Peer("127.0.0.1", 50558));
    peerList.removeIpPort("127.0.0.1", 50558);
    assertEquals(0, peerList.size());
  }

  @Test
  void removeIpPortOnly() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();

    PeerList peerList = serverContext.getPeerList();
    Peer peer = new Peer("127.0.0.1", 50558);
    peerList.add(peer);
    assertTrue(peerList.removeIpPortOnly(peer));
    assertEquals(1, peerList.size());
  }

  @Test
  void size() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    PeerList peerList = serverContext.getPeerList();
    peerList.add(new Peer("127.0.0.1", 50558));
    assertEquals(1, peerList.size());
  }

  @Test
  void clear() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    PeerList peerList = serverContext.getPeerList();
    peerList.add(new Peer("127.0.0.1", 50558));
    peerList.clear();
    assertEquals(0, peerList.size());
  }

  @Test
  void updateKademliaId() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    PeerList peerList = serverContext.getPeerList();

    Peer peer = new Peer("127.0.0.1", 50558);
    NodeId oldId = new NodeId();
    peer.setNodeId(oldId);
    peerList.add(peer);

    assertEquals(1, peerList.size());
    KademliaId newId = new KademliaId();
    peerList.updateKademliaId(peer, newId);

    assertEquals(peer, peerList.get(newId));
    assertEquals(peer.getKademliaId(), newId);
    assertNotEquals(peer.getKademliaId(), oldId.getKademliaId());
  }

  @Test
  void getGoodPeer() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    PeerList peerList = serverContext.getPeerList();
    peerList.add(new Peer("127.0.0.1", 50558));
    Peer goodPeer = peerList.getGoodPeer();
    assertNotNull(goodPeer);
  }

  @Test
  void get() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    PeerList peerList = serverContext.getPeerList();
    ;
    Peer peer = new Peer("127.0.0.2", 50558);
    NodeId id = new NodeId();
    peer.setNodeId(id);
    peerList.add(peer);

    Peer peer2 = new Peer("127.0.0.1", 50558);
    NodeId id2 = new NodeId();
    peer.setNodeId(id2);
    peerList.add(peer2);

    assertEquals(peer, peerList.get(id.getKademliaId()));
  }

  /**
   * M2 regression: {@code add()} used to do its duplicate lookups — {@code
   * peerHashMap.get(kademliaId)} and {@code peerlistIpPort.get(ipPortHash)} — completely outside
   * the ReadWriteLock, and returned early from that fast path without ever taking a lock. Other
   * threads structurally modify those plain HashMaps under the write lock ({@code remove()}, {@code
   * updateKademliaId()}, {@code addPeer()}), so an unsynchronized read during a put/resize/treeify
   * is undefined behaviour (false-negative miss, torn {@code oldPeer}, NPE inside {@code
   * HashMap.get}).
   *
   * <p>The duplicate-by-KademliaId path is the strongest probe: it is the one that used to return
   * without touching a lock at all, so it fails this test before the fix and passes after.
   */
  @Test
  void add_duplicateByKademliaId_stillTakesTheLock() throws Exception {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    PeerList peerList = serverContext.getPeerList();

    NodeId nodeId = new NodeId();
    Peer first = new Peer("127.0.0.9", 50560, nodeId);
    peerList.add(first);

    Peer duplicate = new Peer("127.0.0.9", 50560, nodeId);

    ConcurrencyTestSupport.assertBlockedWhileHeld(
        peerList.getReadWriteLock().writeLock(), () -> peerList.add(duplicate));

    // the duplicate was still rejected once the lock became available
    assertEquals(first, peerList.get(nodeId.getKademliaId()));
  }

  /**
   * Same for the ip+port fast path, which also returned early without a lock (peer without a
   * NodeId, existing entry for the same ip:port).
   */
  @Test
  void add_duplicateByIpPort_stillTakesTheLock() throws Exception {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    PeerList peerList = serverContext.getPeerList();

    peerList.add(new Peer("127.0.0.10", 50561));
    Peer duplicate = new Peer("127.0.0.10", 50561);
    int sizeBefore = peerList.size();

    ConcurrencyTestSupport.assertBlockedWhileHeld(
        peerList.getReadWriteLock().writeLock(), () -> peerList.add(duplicate));

    assertEquals(sizeBefore, peerList.size());
  }
}
