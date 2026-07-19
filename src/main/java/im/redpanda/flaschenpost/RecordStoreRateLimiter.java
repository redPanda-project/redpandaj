package im.redpanda.flaschenpost;

/**
 * T43: a simple global token-bucket rate limiter for inbound channel-rendezvous record stores.
 *
 * <p>Record stores arrive garlic-wrapped (see {@code GarlicRouter} record commands), so the
 * stateless relay cannot attribute them to an authenticated source. A per-source limit is therefore
 * not available on this path; instead a single global cap bounds the DHT-write amplification a
 * flood of stores can cause on this node (each accepted store triggers a {@code KademliaInsertJob}
 * that replicates to two further nodes). Excess stores are dropped silently, exactly like every
 * other best-effort garlic layer.
 *
 * <p>The clock is injectable so the behaviour is deterministic under test.
 */
public final class RecordStoreRateLimiter {

  /** Production defaults: burst of 60 stores, refilled at 1 per second (60/min steady state). */
  public static final int DEFAULT_CAPACITY = 60;

  public static final long DEFAULT_REFILL_INTERVAL_MS = 1000L;

  private final int capacity;
  private final long refillIntervalMs;

  private double tokens;
  private long lastRefillMs;

  public RecordStoreRateLimiter(int capacity, long refillIntervalMs, long nowMs) {
    if (capacity <= 0 || refillIntervalMs <= 0) {
      throw new IllegalArgumentException("capacity and refill interval must be positive");
    }
    this.capacity = capacity;
    this.refillIntervalMs = refillIntervalMs;
    this.tokens = capacity;
    this.lastRefillMs = nowMs;
  }

  /**
   * Tries to consume one token for a record store at {@code nowMs}.
   *
   * @return {@code true} if the store is admitted, {@code false} if the bucket is empty (drop)
   */
  public synchronized boolean tryAcquire(long nowMs) {
    refill(nowMs);
    if (tokens >= 1.0) {
      tokens -= 1.0;
      return true;
    }
    return false;
  }

  private void refill(long nowMs) {
    if (nowMs <= lastRefillMs) {
      return;
    }
    double refilled = (double) (nowMs - lastRefillMs) / refillIntervalMs;
    tokens = Math.min(capacity, tokens + refilled);
    lastRefillMs = nowMs;
  }
}
