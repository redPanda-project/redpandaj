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

  /**
   * Maximum randomized stagger for the very first announce of a <em>freshly registered</em> OH.
   *
   * <p>Deliberately tiny (a couple of seconds) so the first deposits for a brand-new handle become
   * resolvable quickly instead of losing them into the up-to-{@link #ANNOUNCE_STAGGER_MS} window (a
   * sender that reaches the host node before its announce lands is answered OK and then silently
   * dropped, so it waits out its full R-ACK timeout).
   *
   * <p>Anti-profiling note: the full {@link #ANNOUNCE_STAGGER_MS} stagger exists to hide the timing
   * of the <em>periodic</em> re-announces — a stable set of OHs re-announcing on a schedule is a
   * fingerprint. A single fresh registration's announce timing reveals nothing the host node did
   * not already learn from the registration itself, so a prompt announce here is safe. The periodic
   * re-announce path ({@link #work()} → {@link #announceSoon(ServerContext, byte[])}) MUST keep the
   * full stagger.
   */
  static final long NEW_REGISTRATION_STAGGER_MS = Duration.ofSeconds(2).toMillis();

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
    // symmetric jitter: 30 min ± 5 min
    setReRunDelay(BASE_PERIOD_MS - PERIOD_JITTER_MS + rand.nextLong(2 * PERIOD_JITTER_MS + 1));

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
   * Schedules a single announce for {@code ohId} using the full {@link #ANNOUNCE_STAGGER_MS}
   * anti-profiling stagger. This is the periodic re-announce path; for a freshly registered handle
   * use {@link #announceNewRegistration(ServerContext, byte[])} instead so the first deposits are
   * not lost while the announce is still staggered.
   */
  public static void announceSoon(ServerContext serverContext, byte[] ohId) {
    new SingleAnnounceJob(serverContext, ohId, ANNOUNCE_STAGGER_MS).start();
  }

  /**
   * Schedules the very first announce for a newly registered OH with only a small jitter ({@link
   * #NEW_REGISTRATION_STAGGER_MS}) so fresh handles become resolvable promptly instead of waiting
   * out the full stagger (and dropping the sender's first deposits). See {@link
   * #NEW_REGISTRATION_STAGGER_MS} for why the short stagger is safe here while the periodic
   * re-announce must keep the full one.
   */
  public static void announceNewRegistration(ServerContext serverContext, byte[] ohId) {
    new SingleAnnounceJob(serverContext, ohId, NEW_REGISTRATION_STAGGER_MS).start();
  }

  /** One-shot job that builds and inserts the announce record after its randomized delay. */
  static class SingleAnnounceJob extends Job {

    private final byte[] ohId;
    private final long maxStaggerMs;
    private final long delayMs;

    SingleAnnounceJob(ServerContext serverContext, byte[] ohId, long maxStaggerMs) {
      // sample the randomized delay first, then hand it to the primary constructor (super(...)
      // must be the first statement, so the sampling cannot live inline below)
      this(serverContext, ohId, maxStaggerMs, rand.nextLong(maxStaggerMs + 1));
    }

    /** Visible for testing: constructs the job with an explicit (already sampled) delay. */
    SingleAnnounceJob(ServerContext serverContext, byte[] ohId, long maxStaggerMs, long delayMs) {
      // skipImminentRun=true → the single work() run happens after the randomized delay,
      // sampled uniformly from [0, maxStaggerMs]
      super(serverContext, delayMs, false, true);
      this.ohId = ohId;
      this.maxStaggerMs = maxStaggerMs;
      this.delayMs = delayMs;
    }

    /** Visible for testing: the randomized delay this announce was scheduled with. */
    long getDelayMs() {
      return delayMs;
    }

    /** Visible for testing: the stagger bound this announce was created with. */
    long getMaxStaggerMs() {
      return maxStaggerMs;
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
