package im.redpanda.dht;

import im.redpanda.core.ServerContext;
import im.redpanda.ops.Job;
import org.apache.logging.log4j.LogManager;

public class KademliaSearchJobHousekeeper extends Job {

  private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger();

  /**
   * Run interval of this job. The blacklist entries themselves expire after {@link
   * KademliaSearchJob#BLACKLIST_KEY_FOR}, so this interval is the only bound on how many stale
   * entries the map can hold. Entries are created from inbound, peer-controlled search requests, so
   * the sweep has to run often enough that a remote peer cannot grow the map unboundedly. The sweep
   * itself is a single O(n) pass over a small map.
   */
  static final long RUN_INTERVAL = 1000L * 60L;

  /** This Job maintains the kademliaIdSearchBlacklist from the KademliaSearchJob class. */
  public KademliaSearchJobHousekeeper(ServerContext serverContext) {
    super(serverContext, RUN_INTERVAL, true);
  }

  @Override
  public void init() {}

  @Override
  public void work() {

    int removed;
    KademliaSearchJob.getKademliaIdSearchBlacklistLock().lock();
    try {
      long now = System.currentTimeMillis();
      int sizeBefore = KademliaSearchJob.getKademliaIdSearchBlacklist().size();
      // <= now, matching KademliaSearchJob.init(), which treats an entry as expired once
      // `currentTimeMillis - blacklistedTill >= 0`
      KademliaSearchJob.getKademliaIdSearchBlacklist()
          .entrySet()
          .removeIf(entry -> entry.getValue() <= now);
      removed = sizeBefore - KademliaSearchJob.getKademliaIdSearchBlacklist().size();
    } finally {
      KademliaSearchJob.getKademliaIdSearchBlacklistLock().unlock();
    }

    logger.debug("evicted {} expired entries from the KademliaSearchJob blacklist", removed);
  }
}
