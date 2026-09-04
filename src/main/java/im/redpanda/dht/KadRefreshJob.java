package im.redpanda.dht;

import im.redpanda.core.ServerContext;
import im.redpanda.ops.Job;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KadRefreshJob extends Job {

  private static final Logger logger = LogManager.getLogger();

  public KadRefreshJob(ServerContext serverContext) {
    super(serverContext, 1000L * 60L * 60L * 1L, true);
  }

  @Override
  public void init() {}

  @Override
  public void work() {

    logger.debug("refreshing the KadContent");
    serverContext.getKadStoreManager().maintain();
  }
}
