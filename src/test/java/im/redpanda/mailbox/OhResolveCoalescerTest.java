package im.redpanda.mailbox;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import im.redpanda.core.ServerContext;
import im.redpanda.identity.KademliaId;
import im.redpanda.outbound.v1.OhNodeRecord;
import im.redpanda.transport.PeerList;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

/** T162: N1 + review K1–K3, K5 — one network lookup per OH at a time, short positive cache. */
class OhResolveCoalescerTest {

  private static final SecureRandom RANDOM = new SecureRandom();

  private static OhId randomOhId() {
    byte[] bytes = new byte[KademliaId.ID_LENGTH_BYTES];
    RANDOM.nextBytes(bytes);
    return OhId.fromBytes(bytes);
  }

  private static OhNodeRecord record() {
    byte[] node = new byte[KademliaId.ID_LENGTH_BYTES];
    RANDOM.nextBytes(node);
    return OhNodeRecord.newBuilder().setNodeId(ByteString.copyFrom(node)).build();
  }

  private static OhResolveCoalescer.Waiter noop() {
    return new OhResolveCoalescer.Waiter(r -> {}, () -> {});
  }

  private static void waitUntil(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.currentTimeMillis() + 10_000;
    while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
      Thread.sleep(10);
    }
  }

  @Test
  void callersForOneOh_shareOneLookup_andAreAllServed() {
    OhResolveCoalescer coalescer = new OhResolveCoalescer();
    OhId ohId = randomOhId();
    List<OhNodeRecord> resolved = new ArrayList<>();
    AtomicInteger failed = new AtomicInteger();

    OhResolveCoalescer.Lookup lookup = null;
    int starters = 0;
    for (int i = 0; i < 5; i++) {
      OhResolveCoalescer.Joined joined =
          coalescer.join(
              ohId, new OhResolveCoalescer.Waiter(resolved::add, failed::incrementAndGet), 1_000);
      if (joined.mustStart()) {
        starters++;
        lookup = joined.lookup();
      }
    }
    assertThat(starters).as("only the first caller starts a network lookup").isEqualTo(1);
    assertThat(coalescer.waiting(ohId)).isEqualTo(5);

    OhNodeRecord host = record();
    coalescer.complete(ohId, lookup, host, 1_500);

    assertThat(resolved).hasSize(5).containsOnly(host);
    assertThat(failed).hasValue(0);
    assertThat(coalescer.waiting(ohId)).isZero();
  }

  @Test
  void lookupsForDifferentOhs_areIndependent() {
    OhResolveCoalescer coalescer = new OhResolveCoalescer();
    assertThat(coalescer.join(randomOhId(), noop(), 0).mustStart()).isTrue();
    assertThat(coalescer.join(randomOhId(), noop(), 0).mustStart()).isTrue();
    assertThat(coalescer.lookupsStarted()).isEqualTo(2);
  }

  @Test
  void successIsCachedForTheTtl_thenExpires() {
    OhResolveCoalescer coalescer = new OhResolveCoalescer();
    OhId ohId = randomOhId();
    OhResolveCoalescer.Lookup lookup = coalescer.join(ohId, noop(), 0).lookup();
    OhNodeRecord host = record();
    coalescer.complete(ohId, lookup, host, 1_000);

    assertThat(coalescer.cached(ohId, 1_000 + OhResolveCoalescer.RESULT_TTL_MS - 1))
        .isEqualTo(host);
    assertThat(coalescer.cached(ohId, 1_000 + OhResolveCoalescer.RESULT_TTL_MS)).isNull();
  }

  @Test
  void failureIsNotCached_soTheNextCallerLooksUpAgain() {
    OhResolveCoalescer coalescer = new OhResolveCoalescer();
    OhId ohId = randomOhId();
    AtomicInteger failed = new AtomicInteger();
    OhResolveCoalescer.Lookup lookup =
        coalescer
            .join(ohId, new OhResolveCoalescer.Waiter(r -> {}, failed::incrementAndGet), 0)
            .lookup();
    coalescer.complete(ohId, lookup, null, 100);

    assertThat(failed).hasValue(1);
    assertThat(coalescer.cached(ohId, 200)).isNull();
    assertThat(coalescer.join(ohId, noop(), 200).mustStart())
        .as("a failed lookup must be retried for real (T32 race)")
        .isTrue();
  }

  @Test
  void k2_lostLookup_isReplaced_andItsCallersFailInsteadOfBeingInherited() {
    OhResolveCoalescer coalescer = new OhResolveCoalescer();
    OhId ohId = randomOhId();
    AtomicInteger oldFailed = new AtomicInteger();
    AtomicInteger oldResolved = new AtomicInteger();
    OhResolveCoalescer.Waiter old =
        new OhResolveCoalescer.Waiter(
            r -> oldResolved.incrementAndGet(), oldFailed::incrementAndGet);
    OhResolveCoalescer.Lookup stale = coalescer.join(ohId, old, 0).lookup();
    assertThat(coalescer.join(ohId, old, OhResolveCoalescer.MAX_IN_FLIGHT_MS - 1).mustStart())
        .isFalse();

    AtomicInteger newResolved = new AtomicInteger();
    OhResolveCoalescer.Joined fresh =
        coalescer.join(
            ohId,
            new OhResolveCoalescer.Waiter(r -> newResolved.incrementAndGet(), () -> {}),
            OhResolveCoalescer.MAX_IN_FLIGHT_MS);
    assertThat(fresh.mustStart()).isTrue();
    assertThat(oldFailed).as("both callers of the lost lookup fail at replacement").hasValue(2);

    // the lost lookup reports back much later: nobody is served a second time
    coalescer.complete(ohId, stale, record(), OhResolveCoalescer.MAX_IN_FLIGHT_MS + 5);
    assertThat(oldResolved).as("no late, second callback for the old callers").hasValue(0);

    coalescer.complete(ohId, fresh.lookup(), record(), OhResolveCoalescer.MAX_IN_FLIGHT_MS + 10);
    assertThat(newResolved).hasValue(1);
  }

  @Test
  void k1_lateCompletionOfAReplacedLookup_doesNotTouchTheNewerOne() {
    OhResolveCoalescer coalescer = new OhResolveCoalescer();
    OhId ohId = randomOhId();
    OhResolveCoalescer.Lookup stale = coalescer.join(ohId, noop(), 0).lookup();

    AtomicInteger newFailed = new AtomicInteger();
    AtomicInteger newResolved = new AtomicInteger();
    OhResolveCoalescer.Lookup fresh =
        coalescer
            .join(
                ohId,
                new OhResolveCoalescer.Waiter(
                    r -> newResolved.incrementAndGet(), newFailed::incrementAndGet),
                OhResolveCoalescer.MAX_IN_FLIGHT_MS)
            .lookup();

    // the lost lookup finally ends — with a failure
    coalescer.complete(ohId, stale, null, OhResolveCoalescer.MAX_IN_FLIGHT_MS + 5);
    assertThat(newFailed)
        .as("the newer lookup's caller must not get the old lookup's result")
        .hasValue(0);
    assertThat(coalescer.waiting(ohId)).as("the newer entry stays").isEqualTo(1);

    coalescer.complete(ohId, fresh, record(), OhResolveCoalescer.MAX_IN_FLIGHT_MS + 10);
    assertThat(newResolved).hasValue(1);
  }

  @Test
  void k3_waitersPerOhAreCapped_theRestFailAtOnce() {
    OhResolveCoalescer coalescer = new OhResolveCoalescer();
    OhId ohId = randomOhId();
    AtomicInteger failed = new AtomicInteger();
    OhResolveCoalescer.Waiter waiter =
        new OhResolveCoalescer.Waiter(r -> {}, failed::incrementAndGet);
    for (int i = 0; i < OhResolveCoalescer.MAX_WAITERS_PER_OH; i++) {
      assertThat(coalescer.join(ohId, waiter, 0).rejected()).isFalse();
    }
    OhResolveCoalescer.Joined over = coalescer.join(ohId, waiter, 0);
    assertThat(over.rejected()).isTrue();
    assertThat(over.mustStart()).isFalse();
    assertThat(failed).as("the rejected caller failed immediately").hasValue(1);
    assertThat(coalescer.waiting(ohId)).isEqualTo(OhResolveCoalescer.MAX_WAITERS_PER_OH);
  }

  @Test
  void throwingCallback_doesNotStarveTheOthers() {
    OhResolveCoalescer coalescer = new OhResolveCoalescer();
    OhId ohId = randomOhId();
    AtomicInteger served = new AtomicInteger();
    OhResolveCoalescer.Lookup lookup =
        coalescer
            .join(
                ohId,
                new OhResolveCoalescer.Waiter(
                    r -> {
                      throw new IllegalStateException("boom");
                    },
                    () -> {}),
                0)
            .lookup();
    coalescer.join(ohId, new OhResolveCoalescer.Waiter(r -> served.incrementAndGet(), () -> {}), 0);
    coalescer.complete(ohId, lookup, record(), 10);
    assertThat(served).hasValue(1);
  }

  @Test
  void k5_concurrentJoinsWhileAnotherThreadCompletes_everyCallerExactlyOneCallback()
      throws Exception {
    // N threads attach at the same moment while a completer thread ends every lookup it sees
    // started; callers that come after a completion start the next lookup. Every caller must get
    // exactly one callback, and every started lookup must be completed.
    OhResolveCoalescer coalescer = new OhResolveCoalescer();
    OhId ohId = randomOhId();
    int callers = 48; // below MAX_WAITERS_PER_OH, so nobody is rejected
    AtomicIntegerArray callbacks = new AtomicIntegerArray(callers);
    ConcurrentLinkedQueue<OhResolveCoalescer.Lookup> started = new ConcurrentLinkedQueue<>();
    AtomicInteger startedCount = new AtomicInteger();
    CountDownLatch go = new CountDownLatch(1);
    AtomicBoolean joinsDone = new AtomicBoolean();
    ExecutorService pool = Executors.newFixedThreadPool(callers + 1);
    try {
      List<Future<?>> joins = new ArrayList<>();
      for (int i = 0; i < callers; i++) {
        int me = i;
        joins.add(
            pool.submit(
                () -> {
                  go.await();
                  OhResolveCoalescer.Joined joined =
                      coalescer.join(
                          ohId,
                          new OhResolveCoalescer.Waiter(
                              r -> callbacks.incrementAndGet(me),
                              () -> callbacks.incrementAndGet(me)),
                          System.currentTimeMillis());
                  if (joined.mustStart()) {
                    startedCount.incrementAndGet();
                    started.add(joined.lookup());
                  }
                  return null;
                }));
      }
      Future<?> completer =
          pool.submit(
              () -> {
                go.await();
                // interruptible: shutdownNow() must end this loop when a join failed above
                while (!Thread.currentThread().isInterrupted()
                    && (!joinsDone.get() || !started.isEmpty())) {
                  OhResolveCoalescer.Lookup next = started.poll();
                  if (next != null) {
                    coalescer.complete(ohId, next, record(), System.currentTimeMillis());
                  }
                }
                return null;
              });
      go.countDown();
      for (Future<?> join : joins) {
        join.get(10, TimeUnit.SECONDS);
      }
      joinsDone.set(true);
      completer.get(10, TimeUnit.SECONDS);
    } finally {
      pool.shutdownNow();
    }
    for (int i = 0; i < callers; i++) {
      assertThat(callbacks.get(i)).as("callbacks for caller " + i).isEqualTo(1);
    }
    assertThat(coalescer.lookupsStarted()).isEqualTo(startedCount.get());
    assertThat(coalescer.waiting(ohId)).isZero();
  }

  @Test
  void k5_concurrentJoinsWithoutCompletion_startExactlyOneLookup() throws Exception {
    OhResolveCoalescer coalescer = new OhResolveCoalescer();
    OhId ohId = randomOhId();
    int callers = 32;
    CountDownLatch go = new CountDownLatch(1);
    AtomicInteger starters = new AtomicInteger();
    ExecutorService pool = Executors.newFixedThreadPool(callers);
    try {
      List<Future<?>> joins = new ArrayList<>();
      for (int i = 0; i < callers; i++) {
        joins.add(
            pool.submit(
                () -> {
                  go.await();
                  if (coalescer.join(ohId, noop(), System.currentTimeMillis()).mustStart()) {
                    starters.incrementAndGet();
                  }
                  return null;
                }));
      }
      go.countDown();
      for (Future<?> join : joins) {
        join.get(10, TimeUnit.SECONDS);
      }
    } finally {
      pool.shutdownNow();
    }
    assertThat(starters).hasValue(1);
    assertThat(coalescer.waiting(ohId)).isEqualTo(callers);
  }

  @Test
  void resolve_concurrentCallsOnANodeWithoutPeers_eachCallerOneOutcome() throws Exception {
    // End to end through OhResolveJob.resolve with the real job scheduler: no peers, so the
    // lookup fails; the callers arrive from several threads at once.
    ServerContext node = ServerContext.buildDefaultServerContext();
    OhId ohId = randomOhId();
    int callers = 5;
    AtomicInteger failed = new AtomicInteger();
    AtomicInteger resolved = new AtomicInteger();
    CountDownLatch go = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(callers);
    try {
      for (int i = 0; i < callers; i++) {
        pool.submit(
            () -> {
              go.await();
              OhResolveJob.resolve(
                  node, ohId, r -> resolved.incrementAndGet(), failed::incrementAndGet);
              return null;
            });
      }
      go.countDown();
      waitUntil(() -> failed.get() == callers);
    } finally {
      pool.shutdownNow();
    }
    Thread.sleep(200); // a duplicate callback would land here
    assertThat(failed).hasValue(callers);
    assertThat(resolved).hasValue(0);
    // Usually one lookup: the search waits 0–1.5 s of jitter and the callers arrive within
    // microseconds. A jitter sample near zero or a delayed caller thread legitimately starts
    // another one (failures are not cached), so the count is only bounded here; that callers of
    // a lookup in flight share it is pinned by the k5 tests above, which do not depend on timing.
    assertThat(node.getOhResolveCoalescer().lookupsStarted())
        .as("at least one network lookup, never more than one per caller")
        .isBetween(1L, (long) callers);
    assertThat(node.getOhResolveCoalescer().waiting(ohId)).isZero();
  }

  @Test
  void k2_aSearchWhoseInitThrows_stillFailsItsCallers() throws Exception {
    // Job.run catches the exception from init() and only calls done() — OhResolveJob#done must
    // still report the failure, or the callers would wait until the entry is replaced.
    ServerContext node = ServerContext.buildDefaultServerContext();
    node.setPeerList((PeerList) null); // KademliaSearchJob.init() dereferences the peer list
    OhId ohId = randomOhId();
    AtomicInteger failed = new AtomicInteger();
    OhResolveJob.resolve(node, ohId, r -> {}, failed::incrementAndGet);
    waitUntil(() -> failed.get() == 1);
    Thread.sleep(200);
    assertThat(failed).hasValue(1);
    assertThat(node.getOhResolveCoalescer().waiting(ohId)).isZero();
  }
}
