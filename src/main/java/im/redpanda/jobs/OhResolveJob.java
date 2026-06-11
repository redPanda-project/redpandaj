package im.redpanda.jobs;

import im.redpanda.core.KademliaId;
import im.redpanda.core.ServerContext;
import im.redpanda.kademlia.KadContent;
import im.redpanda.outbound.OhDht;
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

  private final byte[] ohId;
  private final Consumer<OhNodeRecord> onResolved;
  private final Runnable onFailed;
  private boolean callbackFired = false;

  OhResolveJob(
      ServerContext serverContext,
      byte[] ohId,
      Consumer<OhNodeRecord> onResolved,
      Runnable onFailed) {
    super(serverContext, OhDht.announceKademliaId(ohId, System.currentTimeMillis()));
    this.ohId = ohId;
    this.onResolved = onResolved;
    this.onFailed = onFailed;
  }

  /**
   * Resolves the host node for {@code ohId}: local KadStore first (no delay), then a randomized
   * delayed DHT search. Exactly one of the callbacks fires.
   */
  public static void resolve(
      ServerContext serverContext,
      byte[] ohId,
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

    new DelayedSearchJob(serverContext, ohId, onResolved, onFailed).start();
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

    private final byte[] ohId;
    private final Consumer<OhNodeRecord> onResolved;
    private final Runnable onFailed;

    DelayedSearchJob(
        ServerContext serverContext,
        byte[] ohId,
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
