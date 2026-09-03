package im.redpanda.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for the accessors {@link PeerList} gained in T115, replacing {@code getPeerArrayList()} +
 * {@code getReadWriteLock()} — the live list and the lock that every caller used to drive itself.
 */
class PeerListSnapshotTest {

  private static Peer peer(String ip, int port) {
    return new Peer(ip, port, NodeId.generateWithSimpleKey());
  }

  @Test
  void snapshot_isACopyInListOrder() {
    PeerList peerList = new PeerList();
    Peer first = peer("10.0.0.1", 59558);
    Peer second = peer("10.0.0.2", 59558);
    peerList.add(first);
    peerList.add(second);

    List<Peer> snapshot = peerList.snapshot();

    assertThat(snapshot).containsExactly(first, second);

    // the caller owns the copy: neither direction leaks
    snapshot.clear();
    assertThat(peerList.size()).isEqualTo(2);
    peerList.add(peer("10.0.0.3", 59558));
    assertThat(peerList.snapshot()).hasSize(3);
  }

  /**
   * The point of handing out a copy: the loops around these snapshots connect sockets, disconnect
   * peers and sleep per peer while network threads keep mutating the list. Iterating the live list
   * without the lock is a {@code ConcurrentModificationException}; iterating it under the lock is
   * what wedged a seed node in T87.
   */
  @Test
  void snapshot_survivesConcurrentMutationDuringIteration() {
    PeerList peerList = new PeerList();
    for (int i = 0; i < 50; i++) {
      peerList.add(peer("10.0.1." + i, 59558));
    }

    List<Peer> snapshot = peerList.snapshot();

    assertThatCode(
            () -> {
              int seen = 0;
              for (Peer ignored : snapshot) {
                peerList.add(peer("10.0.2." + seen, 59558));
                seen++;
              }
              assertThat(seen).isEqualTo(50);
            })
        .doesNotThrowAnyException();
  }

  /** A snapshot must be taken under the read lock, or it can copy a list mid-mutation. */
  @Test
  void snapshot_takesTheReadLock() throws Exception {
    PeerList peerList = new PeerList();
    peerList.add(peer("10.0.0.1", 59558));

    ConcurrencyTestSupport.assertBlockedWhileHeld(
        peerList.getReadWriteLock().writeLock(), peerList::snapshot);
  }

  @Test
  void sortByPriority_putsTheGoodPeersOnTopAndTakesTheWriteLock() throws Exception {
    PeerList peerList = new PeerList();
    Peer disconnected = peer("10.0.0.1", 59558);
    Peer connected = peer("10.0.0.2", 59558);
    connected.setConnected(true);
    peerList.add(disconnected);
    peerList.add(connected);

    ConcurrencyTestSupport.assertBlockedWhileHeld(
        peerList.getReadWriteLock().readLock(), peerList::sortByPriority);

    assertThat(peerList.snapshot())
        .as("Peer.compareTo ranks the connected peer higher")
        .containsExactly(connected, disconnected);
  }
}
