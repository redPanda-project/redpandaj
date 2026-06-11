package im.redpanda.jobs;

import im.redpanda.core.ServerContext;
import im.redpanda.kademlia.KadContent;
import im.redpanda.outbound.OhDht;
import im.redpanda.outbound.OutboundHandleStore;
import java.time.Duration;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * MS02b: periodically announces {@code H(oh_id) → host node} DHT records for every locally
 * registered, non-expired Outbound Handle, so senders that are not directly connected to this node
 * can resolve the OH host (see {@link OhDht}).
 *
 * <p>Re-announcing is required because the Kademlia key rotates with the UTC date and stored
 * content expires. Anti-profiling: every per-OH announce is staggered by a randomized delay so the
 * announce timing does not reveal a stable fingerprint of this node's registered OHs, and the job
 * period itself is jittered.
 */
public class OhAnnounceJob extends Job {

  private static final Logger logger = LogManager.getLogger();

  static final long BASE_PERIOD_MS = Duration.ofMinutes(30).toMillis();
  static final long PERIOD_JITTER_MS = Duration.ofMinutes(5).toMillis();

  /** Maximum randomized stagger for a single announce within one job run. */
  static final long ANNOUNCE_STAGGER_MS = Duration.ofSeconds(30).toMillis();

  /** First run shortly after startup (jobs run immediately otherwise). */
  private static final long INITIAL_DELAY_MS = Duration.ofSeconds(20).toMillis();

  public OhAnnounceJob(ServerContext serverContext) {
    super(serverContext, INITIAL_DELAY_MS, true, true);
  }

  @Override
  public void init() {
    // no setup needed
  }

  @Override
  public void work() {
    setReRunDelay(BASE_PERIOD_MS + rand.nextLong(PERIOD_JITTER_MS));

    OutboundHandleStore handleStore = serverContext.getOutboundHandleStore();
    if (handleStore == null) {
      return;
    }
    List<byte[]> ohIds = handleStore.listActiveOhIds(System.currentTimeMillis());
    for (byte[] ohId : ohIds) {
      announceSoon(serverContext, ohId);
    }
    if (!ohIds.isEmpty()) {
      logger.debug("scheduled OH announce for {} handles", ohIds.size());
    }
  }

  /**
   * Schedules a single announce for {@code ohId} after a randomized delay (anti-profiling stagger).
   * Also used directly after a successful RegisterOh so fresh handles become resolvable without
   * waiting for the next periodic run.
   */
  public static void announceSoon(ServerContext serverContext, byte[] ohId) {
    new SingleAnnounceJob(serverContext, ohId).start();
  }

  /** One-shot job that builds and inserts the announce record after its randomized delay. */
  static class SingleAnnounceJob extends Job {

    private final byte[] ohId;

    SingleAnnounceJob(ServerContext serverContext, byte[] ohId) {
      // skipImminentRun=true → the single work() run happens after the randomized delay
      super(serverContext, 1 + rand.nextLong(ANNOUNCE_STAGGER_MS), false, true);
      this.ohId = ohId;
    }

    @Override
    public void init() {
      // no setup needed
    }

    @Override
    public void work() {
      try {
        KadContent content =
            OhDht.buildAnnounceContent(ohId, serverContext.getNonce(), System.currentTimeMillis());
        new KademliaInsertJob(serverContext, content).start();
      } finally {
        done();
      }
    }
  }
}
