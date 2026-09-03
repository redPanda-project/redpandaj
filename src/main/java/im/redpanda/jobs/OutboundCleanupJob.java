package im.redpanda.jobs;

import im.redpanda.core.ServerContext;
import im.redpanda.outbound.OutboundStore;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Periodically expires Outbound Handles: a handle whose lease ran out is removed together with its
 * mailbox.
 *
 * <p>T109: this is no longer a re-alignment of two databases — {@link
 * OutboundStore#cleanupExpiredHandles(long)} removes both in one transaction.
 */
public class OutboundCleanupJob extends Job {

  private static final Logger logger = LogManager.getLogger();
  private final OutboundStore outboundStore;

  public OutboundCleanupJob(ServerContext serverContext) {
    super(serverContext, TimeUnit.MINUTES.toMillis(10), true); // Permanent job, run every 10 mins
    this.outboundStore = serverContext.getOutboundStore();
  }

  @Override
  public void init() {
    // No specific init needed
  }

  @Override
  public void work() {
    try {
      if (outboundStore != null) {
        outboundStore.cleanupExpiredHandles(System.currentTimeMillis());
      }
    } catch (Exception e) {
      logger.error("Error in OutboundCleanupJob", e);
    }
  }
}
