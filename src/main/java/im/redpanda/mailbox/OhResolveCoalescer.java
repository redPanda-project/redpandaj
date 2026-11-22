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
 *       instead of starting another one; its outcome serves them all.
 *   <li><b>Positive result cache</b> ({@link #RESULT_TTL_MS}): a resolved host is reused for a
 *       short time, so deposits a few seconds apart do not search again. Only successes are cached
 *       — a failed lookup (e.g. the announce of a fresh OH not landed yet, T32) must be retried for
 *       real. The TTL bounds staleness: an OH never changes its host under the same id (renewal
 *       re-registers on the same node; failover registers a new id), and a cached host that no
 *       longer holds the OH answers with its own MS02b forward / HANDLE_EXPIRED exactly as for a
 *       stale DHT record.
 * </ul>
 *
 * <p>Callbacks are always invoked outside the lock.
 */
public final class OhResolveCoalescer {

  private static final Logger logger = LogManager.getLogger();

  /** How long a resolved OH host is reused without a new lookup. */
  static final long RESULT_TTL_MS = 30_000;

  /**
   * An in-flight lookup older than this is treated as lost (a lookup ends after at most ~1.5 s
   * jitter + the 5 s search timeout); the next caller starts a fresh one and inherits its waiters,
   * so a lookup that never reports back cannot block an OH forever.
   */
  static final long MAX_IN_FLIGHT_MS = 20_000;

  /** Bound on cached results; expired entries are pruned first, then caching is skipped. */
  static final int MAX_CACHED = 4096;

  record Waiter(Consumer<OhNodeRecord> onResolved, Runnable onFailed) {}

  private record Cached(OhNodeRecord record, long expiresAtMs) {}

  private final Object lock = new Object();

  private record InFlight(List<Waiter> waiters, long startedAtMs) {}

  private final Map<OhId, InFlight> inFlight = new HashMap<>();
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
   * Registers {@code waiter} for {@code ohId}. Returns {@code true} if no lookup is in flight — the
   * caller must then start one and report its outcome via {@link #complete}; {@code false} if the
   * waiter was attached to the running lookup.
   */
  boolean join(OhId ohId, Waiter waiter, long nowMs) {
    synchronized (lock) {
      InFlight running = inFlight.get(ohId);
      if (running != null && nowMs - running.startedAtMs() < MAX_IN_FLIGHT_MS) {
        running.waiters().add(waiter);
        return false;
      }
      List<Waiter> waiters = new ArrayList<>();
      if (running != null) {
        logger.debug("OH lookup for {} never reported back, starting a new one", ohId);
        waiters.addAll(running.waiters());
      }
      waiters.add(waiter);
      inFlight.put(ohId, new InFlight(waiters, nowMs));
      lookupsStarted++;
      return true;
    }
  }

  /**
   * Ends the in-flight lookup for {@code ohId}: caches a successful {@code record}, removes the
   * entry and serves every attached waiter ({@code record == null} means failure).
   */
  void complete(OhId ohId, OhNodeRecord record, long nowMs) {
    List<Waiter> waiters;
    synchronized (lock) {
      InFlight done = inFlight.remove(ohId);
      waiters = done == null ? null : done.waiters();
      if (record != null) {
        if (cache.size() >= MAX_CACHED) {
          pruneExpired(nowMs);
        }
        if (cache.size() < MAX_CACHED) {
          cache.put(ohId, new Cached(record, nowMs + RESULT_TTL_MS));
        }
      }
    }
    if (waiters == null) {
      return;
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
      InFlight running = inFlight.get(ohId);
      return running == null ? 0 : running.waiters().size();
    }
  }
}
