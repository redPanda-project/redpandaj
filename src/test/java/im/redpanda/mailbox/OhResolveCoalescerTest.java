package im.redpanda.mailbox;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import im.redpanda.core.ServerContext;
import im.redpanda.identity.KademliaId;
import im.redpanda.outbound.v1.OhNodeRecord;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** T162: N1 — one network lookup per OH at a time, short positive result cache. */
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

  @Test
  void concurrentCallersForOneOh_shareOneLookup_andAreAllServed() {
    OhResolveCoalescer coalescer = new OhResolveCoalescer();
    OhId ohId = randomOhId();
    List<OhNodeRecord> resolved = new ArrayList<>();
    AtomicInteger failed = new AtomicInteger();

    int starters = 0;
    for (int i = 0; i < 5; i++) {
      if (coalescer.join(
          ohId, new OhResolveCoalescer.Waiter(resolved::add, failed::incrementAndGet), 1_000)) {
        starters++;
      }
    }
    assertThat(starters).as("only the first caller starts a network lookup").isEqualTo(1);
    assertThat(coalescer.lookupsStarted()).isEqualTo(1);
    assertThat(coalescer.waiting(ohId)).isEqualTo(5);

    OhNodeRecord host = record();
    coalescer.complete(ohId, host, 1_500);

    assertThat(resolved).hasSize(5).containsOnly(host);
    assertThat(failed).hasValue(0);
    assertThat(coalescer.waiting(ohId)).isZero();
  }

  @Test
  void lookupsForDifferentOhs_areIndependent() {
    OhResolveCoalescer coalescer = new OhResolveCoalescer();
    OhResolveCoalescer.Waiter noop = new OhResolveCoalescer.Waiter(r -> {}, () -> {});
    assertThat(coalescer.join(randomOhId(), noop, 0)).isTrue();
    assertThat(coalescer.join(randomOhId(), noop, 0)).isTrue();
    assertThat(coalescer.lookupsStarted()).isEqualTo(2);
  }

  @Test
  void successIsCachedForTheTtl_thenExpires() {
    OhResolveCoalescer coalescer = new OhResolveCoalescer();
    OhId ohId = randomOhId();
    coalescer.join(ohId, new OhResolveCoalescer.Waiter(r -> {}, () -> {}), 0);
    OhNodeRecord host = record();
    coalescer.complete(ohId, host, 1_000);

    assertThat(coalescer.cached(ohId, 1_000 + OhResolveCoalescer.RESULT_TTL_MS - 1))
        .isEqualTo(host);
    assertThat(coalescer.cached(ohId, 1_000 + OhResolveCoalescer.RESULT_TTL_MS)).isNull();
  }

  @Test
  void failureIsNotCached_soTheNextCallerLooksUpAgain() {
    OhResolveCoalescer coalescer = new OhResolveCoalescer();
    OhId ohId = randomOhId();
    AtomicInteger failed = new AtomicInteger();
    coalescer.join(ohId, new OhResolveCoalescer.Waiter(r -> {}, failed::incrementAndGet), 0);
    coalescer.complete(ohId, null, 100);

    assertThat(failed).hasValue(1);
    assertThat(coalescer.cached(ohId, 200)).isNull();
    assertThat(coalescer.join(ohId, new OhResolveCoalescer.Waiter(r -> {}, () -> {}), 200))
        .as("a failed lookup must be retried for real (T32 race)")
        .isTrue();
  }

  @Test
  void lookupThatNeverReportsBack_isReplaced_andItsWaitersInherited() {
    OhResolveCoalescer coalescer = new OhResolveCoalescer();
    OhId ohId = randomOhId();
    AtomicInteger resolved = new AtomicInteger();
    OhResolveCoalescer.Waiter waiter =
        new OhResolveCoalescer.Waiter(r -> resolved.incrementAndGet(), () -> {});
    assertThat(coalescer.join(ohId, waiter, 0)).isTrue();
    assertThat(coalescer.join(ohId, waiter, OhResolveCoalescer.MAX_IN_FLIGHT_MS - 1)).isFalse();
    assertThat(coalescer.join(ohId, waiter, OhResolveCoalescer.MAX_IN_FLIGHT_MS)).isTrue();

    coalescer.complete(ohId, record(), OhResolveCoalescer.MAX_IN_FLIGHT_MS + 10);
    assertThat(resolved).as("all three callers served by the replacement lookup").hasValue(3);
  }

  @Test
  void throwingCallback_doesNotStarveTheOthers() {
    OhResolveCoalescer coalescer = new OhResolveCoalescer();
    OhId ohId = randomOhId();
    AtomicInteger served = new AtomicInteger();
    coalescer.join(
        ohId,
        new OhResolveCoalescer.Waiter(
            r -> {
              throw new IllegalStateException("boom");
            },
            () -> {}),
        0);
    coalescer.join(ohId, new OhResolveCoalescer.Waiter(r -> served.incrementAndGet(), () -> {}), 0);
    coalescer.complete(ohId, record(), 10);
    assertThat(served).hasValue(1);
  }

  @Test
  void resolve_concurrentCallsOnANodeWithoutPeers_eachGetExactlyOneOutcome()
      throws InterruptedException {
    // End to end through OhResolveJob.resolve with the real job scheduler: no peers, so the
    // lookup fails; every caller must get its failure callback exactly once and none may hang.
    ServerContext node = ServerContext.buildDefaultServerContext();
    OhId ohId = randomOhId();
    AtomicInteger failed = new AtomicInteger();
    AtomicInteger resolved = new AtomicInteger();
    for (int i = 0; i < 5; i++) {
      OhResolveJob.resolve(node, ohId, r -> resolved.incrementAndGet(), failed::incrementAndGet);
    }
    long deadline = System.currentTimeMillis() + 10_000;
    while (failed.get() < 5 && System.currentTimeMillis() < deadline) {
      Thread.sleep(20);
    }
    Thread.sleep(200); // a duplicate callback would land here
    assertThat(failed).hasValue(5);
    assertThat(resolved).hasValue(0);
    assertThat(node.getOhResolveCoalescer().waiting(ohId)).isZero();
  }
}
