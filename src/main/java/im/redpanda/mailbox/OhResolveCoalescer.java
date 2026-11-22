package im.redpanda.mailbox;

import im.redpanda.outbound.v1.OhNodeRecord;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * T162: per-node bookkeeping that bounds the DHT lookups {@link OhResolveJob} issues for one OH.
 *
 * <p>Every deposit this node forwards for an OH it does not host (MS02b) needs the OH's host. A
 * reverse-garlic reply, the R-ACK of the message it answers and the redundant copies of a send all
 * target the same OH within milliseconds, so without bookkeeping each of them started its own
 * network search (up to ~20 KADEMLIA_GETs each on the shared job scheduler), and the peers asked by
 * the first search had already blacklisted the key for the second one.
 *
 * <ul>
 *   <li><b>Coalescing:</b> while a lookup for an OH is in flight, further callers attach to it
 *       instead of starting another one; its outcome serves exactly the callers attached to it
 *       (each lookup owns its {@link Lookup} marker, review K1). At most {@link
 *       #MAX_WAITERS_PER_OH} callers wait per OH; beyond that a caller fails at once (K3) — this
 *       node accepts deposits from foreign nodes, and every waiter holds a deposit payload.
 *   <li><b>Positive result cache</b> ({@link #RESULT_TTL_MS}): a resolved host is reused for a
 *       short time, so the deposits of one conversation burst do not search again. Only successes
 *       are cached — a failed lookup (e.g. the announce of a fresh OH not landed yet, T32) must be
 *       retried for real. An OH id <em>can</em> move to another host: a client re-registers a lost
 *       handle under the same id with the first node it reaches when it knows no endpoint ({@code
 *       reregisterLostHandle}), and an address may come to belong to another node. For the TTL a
 *       cached old host then keeps receiving the deposits; it does not hold the OH and forwards
 *       (MS02b, bounded by {@link im.redpanda.routing.OhForwarder#MAX_HOPS}) or answers
 *       HANDLE_EXPIRED, so the sender re-sends — a delay, no loss. The TTL is kept short for that
 *       reason (review K6).
 *   <li><b>Lost lookups:</b> a lookup older than {@link #MAX_IN_FLIGHT_MS} that never reported back
 *       is replaced by the next caller; its own callers fail at that point instead of being served
 *       much later with a long-expired return path (K2). {@link OhResolveJob} itself guarantees a
 *       callback on every ending (see {@code OhResolveJob#done()}).
 * </ul>
 *
 * <p>Callbacks are always invoked outside the lock.
 */
public final class OhResolveCoalescer {

  private static final Logger logger = LogManager.getLogger();

  /** How long a resolved OH host is reused without a new lookup (covers one conversation burst). */
  static final long RESULT_TTL_MS = 10_000;

  /**
   * A lookup older than this is treated as lost (a lookup ends after at most ~1.5 s jitter + the 5
   * s search timeout); the next caller starts a fresh one.
   */
  static final long MAX_IN_FLIGHT_MS = 20_000;

  /**
   * Callers that may wait for one OH's lookup (mirrors {@code OhForwarder.MAX_PENDING_RETRIES}).
   */
  static final int MAX_WAITERS_PER_OH = 64;

  /** Bound on cached results; expired entries are pruned first, then caching is skipped. */
  static final int MAX_CACHED = 4096;

  record Waiter(Consumer<OhNodeRecord> onResolved, Runnable onFailed) {}

  private record Cached(OhNodeRecord record, long expiresAtMs) {}

  /** One network lookup and the callers attached to it; also the marker passed to complete. */
  static final class Lookup {
    private final List<Waiter> waiters = new ArrayList<>();
    private final long startedAtMs;
    private boolean closed = false;

    private Lookup(long startedAtMs) {
      this.startedAtMs = startedAtMs;
    }
  }

  /** Outcome of {@link #join}: start a lookup ({@code lookup} set), attached, or rejected. */
  record Joined(Lookup lookup, boolean rejected) {
    boolean mustStart() {
      return lookup != null;
    }
  }

  private static final Joined ATTACHED = new Joined(null, false);
  private static final Joined REJECTED = new Joined(null, true);

  private final Object lock = new Object();
  private final Map<OhId, Lookup> inFlight = new HashMap<>();
  private final Map<OhId, Cached> cache = new HashMap<>();
  private long lookupsStarted = 0;

  /** The cached host record for {@code ohId}, or {@code null} if none or expired. */
  OhNodeRecord cached(OhId ohId, long nowMs) {
    synchronized (lock) {
      Cached entry = cache.get(ohId);
      if (entry == null) {
        return null;
      }
      if (nowMs >= entry.expiresAtMs()) {
        cache.remove(ohId);
        return null;
      }
      return entry.record();
    }
  }

  /**
   * Registers {@code waiter} for {@code ohId}.
   *
   * <ul>
   *   <li>{@link Joined#mustStart()}: no usable lookup is in flight — the caller must start one and
   *       report its outcome via {@link #complete} with the returned marker;
   *   <li>attached to the running lookup (nothing to do);
   *   <li>{@link Joined#rejected()}: {@link #MAX_WAITERS_PER_OH} callers already wait — {@code
   *       waiter.onFailed} has already been run.
   * </ul>
   *
   * A lookup older than {@link #MAX_IN_FLIGHT_MS} is replaced; its callers are failed here.
   */
  Joined join(OhId ohId, Waiter waiter, long nowMs) {
    List<Waiter> abandoned = List.of();
    Joined result;
    synchronized (lock) {
      Lookup running = inFlight.get(ohId);
      if (running != null && nowMs - running.startedAtMs < MAX_IN_FLIGHT_MS) {
        if (running.waiters.size() >= MAX_WAITERS_PER_OH) {
          result = REJECTED;
        } else {
          running.waiters.add(waiter);
          result = ATTACHED;
        }
      } else {
        if (running != null) {
          logger.debug("OH lookup for {} never reported back, starting a new one", ohId);
          running.closed = true;
          abandoned = List.copyOf(running.waiters);
          running.waiters.clear();
        }
        Lookup lookup = new Lookup(nowMs);
        lookup.waiters.add(waiter);
        inFlight.put(ohId, lookup);
        lookupsStarted++;
        result = new Joined(lookup, false);
      }
    }
    for (Waiter old : abandoned) {
      fail(ohId, old);
    }
    if (result.rejected()) {
      fail(ohId, waiter);
    }
    return result;
  }

  /**
   * Ends {@code lookup} for {@code ohId}: caches a successful {@code record} and serves exactly the
   * callers attached to this lookup ({@code record == null} means failure). A lookup that was
   * replaced meanwhile serves nobody (its callers were failed at replacement) and leaves the newer
   * lookup's entry alone.
   */
  void complete(OhId ohId, Lookup lookup, OhNodeRecord record, long nowMs) {
    List<Waiter> waiters;
    synchronized (lock) {
      if (record != null) {
        if (cache.size() >= MAX_CACHED) {
          pruneExpired(nowMs);
        }
        if (cache.size() < MAX_CACHED) {
          cache.put(ohId, new Cached(record, nowMs + RESULT_TTL_MS));
        }
      }
      if (lookup.closed) {
        return;
      }
      lookup.closed = true;
      inFlight.remove(ohId, lookup);
      waiters = List.copyOf(lookup.waiters);
      lookup.waiters.clear();
    }
    for (Waiter waiter : waiters) {
      // one failing callback (e.g. a routing error for one deposit) must not starve the others
      try {
        if (record != null) {
          waiter.onResolved().accept(record);
        } else {
          waiter.onFailed().run();
        }
      } catch (RuntimeException e) {
        logger.debug("OH resolve callback for {} failed: {}", ohId, e.getMessage());
      }
    }
  }

  private static void fail(OhId ohId, Waiter waiter) {
    try {
      waiter.onFailed().run();
    } catch (RuntimeException e) {
      logger.debug("OH resolve failure callback for {} failed: {}", ohId, e.getMessage());
    }
  }

  private void pruneExpired(long nowMs) {
    Iterator<Cached> it = cache.values().iterator();
    while (it.hasNext()) {
      if (nowMs >= it.next().expiresAtMs()) {
        it.remove();
      }
    }
  }

  /** Visible for testing: number of network lookups started (first joiners). */
  long lookupsStarted() {
    synchronized (lock) {
      return lookupsStarted;
    }
  }

  /** Visible for testing: callers currently attached to the lookup for {@code ohId}. */
  int waiting(OhId ohId) {
    synchronized (lock) {
      Lookup running = inFlight.get(ohId);
      return running == null ? 0 : running.waiters.size();
    }
  }
}
