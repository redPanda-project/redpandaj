package im.redpanda.jobs;

import im.redpanda.core.KademliaId;
import im.redpanda.kademlia.KadStoreManager;

import java.util.HashMap;
import java.util.Map;

public class KademliaSearchJobHousekeeper extends Job {

    /**
     * This Job maintains the kademliaIdSearchBlacklist from the KademliaSearchJob class.
     */
    public KademliaSearchJobHousekeeper() {
        permanent = true;
        reRunDelay = 1000L * 60L * 10L; // 10 mins
    }

    @Override
    public void init() {
    }

    @Override
    public void work() {

        System.out.println("refresh the KadContent");

        KademliaSearchJob.getKademliaIdSearchBlacklistLock().lock();
        try {
            HashMap<KademliaId, Long> map = KademliaSearchJob.getKademliaIdSearchBlacklist();

            for (Map.Entry<KademliaId, Long> e : map.entrySet()) {

            }

        } finally {
            KademliaSearchJob.getKademliaIdSearchBlacklistLock().unlock();
        }


    }
}
