package im.redpanda.jobs;

import im.redpanda.core.KademliaId;
import im.redpanda.core.ServerContext;
import org.apache.logging.log4j.LogManager;

import java.util.HashMap;
import java.util.Map;

public class KademliaSearchJobHousekeeper extends Job {

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger();

    /**
     * This Job maintains the kademliaIdSearchBlacklist from the KademliaSearchJob class.
     */
    public KademliaSearchJobHousekeeper(ServerContext serverContext) {
        super(serverContext, 1000L * 60L * 10L, true);
    }

    @Override
    public void init() {
    }

    @Override
    public void work() {

        logger.info("refresh the KadContent");

        KademliaSearchJob.getKademliaIdSearchBlacklistLock().lock();
        try {
            HashMap<KademliaId, Long> map = KademliaSearchJob.getKademliaIdSearchBlacklist();

            for (Map.Entry<KademliaId, Long> e : map.entrySet()) {
                //todo
            }

        } finally {
            KademliaSearchJob.getKademliaIdSearchBlacklistLock().unlock();
        }


    }
}
