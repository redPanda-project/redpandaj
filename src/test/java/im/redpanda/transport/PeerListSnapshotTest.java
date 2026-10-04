package im.redpanda.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import im.redpanda.identity.NodeId;
import im.redpanda.testutil.ConcurrencyTestSupport;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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

  /**
   * TD106: the documented failure mode of {@link PeerList#sortByPriority()}. {@link
   * Peer#getPriority()} reads mutable state ({@code connected}, {@code retries}, the node's test
   * counters) that other threads change while the sort runs, so the JDK sort ({@code
   * ComparableTimSort}) can detect an inconsistent ordering and throw an {@link
   * IllegalArgumentException}. The sort must let it out (callers skip the round) and must not leave
   * the write lock held, or every later peer list access from another thread would wedge.
   *
   * <p>Whether the JDK sort detects a given inconsistent sequence is implementation-dependent, so
   * the exception is raised deterministically from inside the comparison instead: the 50th priority
   * read throws exactly what the sort would.
   */
  @Test
  void sortByPriority_propagatesSortContractViolationAndReleasesTheWriteLock() throws Exception {
    PeerList peerList = new PeerList();
    AtomicInteger priorityReads = new AtomicInteger();
    for (int i = 0; i < 100; i++) {
      peerList.add(
          new Peer("10.0.3." + i, 59558, NodeId.generateWithSimpleKey()) {
            @Override
            public int getPriority() {
              if (priorityReads.incrementAndGet() == 50) {
                throw new IllegalArgumentException(
                    "Comparison method violates its general contract!");
              }
              return super.getPriority();
            }
          });
    }

    assertThatThrownBy(peerList::sortByPriority)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Comparison method violates its general contract");

    ExecutorService other = Executors.newSingleThreadExecutor();
    try {
      Future<Boolean> acquired =
          other.submit(
              () -> {
                boolean locked = peerList.getReadWriteLock().writeLock().tryLock();
                if (locked) {
                  peerList.getReadWriteLock().writeLock().unlock();
                }
                return locked;
              });
      assertThat(acquired.get(10, TimeUnit.SECONDS))
          .as("another thread gets the write lock after the failed sort")
          .isTrue();
    } finally {
      other.shutdownNow();
    }
  }
}
