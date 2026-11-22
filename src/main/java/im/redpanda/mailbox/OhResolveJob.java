package im.redpanda.mailbox;

import im.redpanda.core.ServerContext;
import im.redpanda.dht.KadContent;
import im.redpanda.dht.KademliaSearchJob;
import im.redpanda.identity.KademliaId;
import im.redpanda.ops.Job;
import im.redpanda.outbound.v1.OhNodeRecord;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * MS02b: resolves {@code oh_id → host node} via the DHT announce record (see {@link OhDht}).
 *
 * <p>Anti-profiling: the network search starts only after a randomized delay, so the timing of a
 * lookup does not directly correlate with the FlaschenpostPut that triggered it. The local store is
 * checked without delay (a local read leaks nothing). Records are fixed-size (padded), so the
 * answer size is uniform as well.
 */
public class OhResolveJob extends KademliaSearchJob {

  private static final Logger logger = LogManager.getLogger();

  /** Upper bound for the randomized delay before the DHT search is issued. */
  static final long LOOKUP_DELAY_JITTER_MS = Duration.ofMillis(1500).toMillis();

  private final OhId ohId;
  private final Consumer<OhNodeRecord> onResolved;
  private final Runnable onFailed;
  private boolean callbackFired = false;

  OhResolveJob(
      ServerContext serverContext,
      OhId ohId,
      Consumer<OhNodeRecord> onResolved,
      Runnable onFailed) {
    super(serverContext, OhDht.announceKademliaId(ohId, System.currentTimeMillis()));
    this.ohId = ohId;
    this.onResolved = onResolved;
    this.onFailed = onFailed;
  }

  /**
   * Resolves the host node for {@code ohId}: local KadStore first (no delay), then a host resolved
   * within the last {@link OhResolveCoalescer#RESULT_TTL_MS}, then a randomized delayed DHT search
   * — one per OH at a time: callers arriving while it runs attach to it (T162, see {@link
   * OhResolveCoalescer}). Exactly one of the callbacks fires.
   */
  public static void resolve(
      ServerContext serverContext,
      OhId ohId,
      Consumer<OhNodeRecord> onResolved,
      Runnable onFailed) {
    long now = System.currentTimeMillis();
    KademliaId announceId = OhDht.announceKademliaId(ohId, now);

    KadContent local = serverContext.getKadStoreManager().get(announceId);
    if (local != null) {
      OhNodeRecord record = OhDht.extractValidRecord(List.of(local), ohId, now);
      if (record != null) {
        onResolved.accept(record);
        return;
      }
    }

    OhResolveCoalescer coalescer = serverContext.getOhResolveCoalescer();
    OhNodeRecord cached = coalescer.cached(ohId, now);
    if (cached != null) {
      onResolved.accept(cached);
      return;
    }
    if (!coalescer.join(ohId, new OhResolveCoalescer.Waiter(onResolved, onFailed), now)) {
      return; // attached to the lookup already in flight for this OH
    }
    try {
      new DelayedSearchJob(
              serverContext,
              ohId,
              record -> coalescer.complete(ohId, record, System.currentTimeMillis()),
              () -> coalescer.complete(ohId, null, System.currentTimeMillis()))
          .start();
    } catch (RuntimeException e) {
      // scheduling failed (e.g. executor shutting down): fail every attached caller now instead of
      // leaving them to the in-flight timeout
      logger.debug("could not schedule OH lookup for {}: {}", ohId, e.getMessage());
      coalescer.complete(ohId, null, System.currentTimeMillis());
    }
  }

  @Override
  protected ArrayList<KadContent> success() {
    ArrayList<KadContent> contents = super.success();
    OhNodeRecord record = OhDht.extractValidRecord(contents, ohId, System.currentTimeMillis());
    if (record != null) {
      fireResolved(record);
    } else {
      fireFailed();
    }
    return contents;
  }

  @Override
  protected void fail() {
    fireFailed();
  }

  /**
   * T162: every forwarded deposit resolves its OH, and a reverse-garlic reply and the R-ACK of the
   * message it answers target the same OH at the same exit node milliseconds apart. Rejecting the
   * second lookup on the anti-loop blacklist dropped the reply after the parked retry hit the same
   * entry (HANDLE_EXPIRED after 15–35 s). Origin lookups are never part of a search loop.
   *
   * <p>Still needed next to the coalescing in {@link #resolve}: concurrent lookups are merged
   * there, but a lookup that starts after the previous one for the same key ended — the T32 retry
   * 15 s after a failed lookup (failures are not cached), or any lookup while a peer-triggered
   * search ({@code KademliaSearchJobAnswerPeer}) holds the key — would otherwise still be rejected
   * inside the 30 s window.
   */
  @Override
  protected boolean rejectsBlacklistedKey() {
    return false;
  }

  private synchronized void fireResolved(OhNodeRecord record) {
    if (!callbackFired) {
      callbackFired = true;
      onResolved.accept(record);
    }
  }

  private synchronized void fireFailed() {
    if (!callbackFired) {
      callbackFired = true;
      onFailed.run();
    }
  }

  /** One-shot job adding the randomized anti-profiling delay before the actual DHT search. */
  static class DelayedSearchJob extends Job {

    private final OhId ohId;
    private final Consumer<OhNodeRecord> onResolved;
    private final Runnable onFailed;

    DelayedSearchJob(
        ServerContext serverContext,
        OhId ohId,
        Consumer<OhNodeRecord> onResolved,
        Runnable onFailed) {
      // delay sampled uniformly from [0, LOOKUP_DELAY_JITTER_MS]
      super(serverContext, rand.nextLong(LOOKUP_DELAY_JITTER_MS + 1), false, true);
      this.ohId = ohId;
      this.onResolved = onResolved;
      this.onFailed = onFailed;
    }

    @Override
    public void init() {
      // no setup needed
    }

    @Override
    public void work() {
      try {
        new OhResolveJob(serverContext, ohId, onResolved, onFailed).start();
        logger.debug("started delayed OH resolve search");
      } finally {
        done();
      }
    }
  }
}
