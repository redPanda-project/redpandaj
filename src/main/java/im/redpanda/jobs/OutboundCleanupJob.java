package im.redpanda.jobs;

import im.redpanda.core.ServerContext;
import im.redpanda.outbound.OutboundHandleStore;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Periodically cleans up expired Outbound Handles and potentially old mailbox items. */
public class OutboundCleanupJob extends Job {

  private static final Logger logger = LogManager.getLogger();
  private final OutboundHandleStore handleStore;

  public OutboundCleanupJob(ServerContext serverContext) {
    super(serverContext, TimeUnit.MINUTES.toMillis(10), true); // Permanent job, run every 10 mins
    this.handleStore = serverContext.getOutboundHandleStore();
  }

  @Override
  public void init() {
    // No specific init needed
  }

  @Override
  public void work() {
    try {
      long now = System.currentTimeMillis();
      if (handleStore != null) {
        handleStore.cleanupExpired(now);
      }

      // Mailbox cleanup could go here

    } catch (Exception e) {
      logger.error("Error in OutboundCleanupJob", e);
    }
  }
}
