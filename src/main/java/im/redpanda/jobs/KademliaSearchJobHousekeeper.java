package im.redpanda.jobs;

import im.redpanda.core.ServerContext;
import org.apache.logging.log4j.LogManager;

public class KademliaSearchJobHousekeeper extends Job {

  private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger();

  /** This Job maintains the kademliaIdSearchBlacklist from the KademliaSearchJob class. */
  public KademliaSearchJobHousekeeper(ServerContext serverContext) {
    super(serverContext, 1000L * 60L * 10L, true);
  }

  @Override
  public void init() {}

  @Override
  public void work() {

    logger.info("refresh the KadContent");

    KademliaSearchJob.getKademliaIdSearchBlacklistLock().lock();
    try {
      // todo: remove expired entries
      KademliaSearchJob.getKademliaIdSearchBlacklist()
          .entrySet()
          .removeIf(entry -> entry.getValue() < System.currentTimeMillis());

    } finally {
      KademliaSearchJob.getKademliaIdSearchBlacklistLock().unlock();
    }
  }
}
