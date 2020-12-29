package im.redpanda.jobs;

import im.redpanda.core.Peer;
import im.redpanda.core.PeerList;
import im.redpanda.flaschenpost.FPStoreManager;

public class PeerPerformanceTestSchedulerJob extends Job {


    public PeerPerformanceTestSchedulerJob() {
        super(1000L * 10L * 1L, true);
    }

    @Override
    public void init() {

    }

    @Override
    public void work() {

        Peer goodPeer = PeerList.getGoodPeer(1.0f); //todo change later if network is big enough

        if (goodPeer == null) {
            return;
        }

//        new PeerPerformanceTestFlaschenpostJob(goodPeer).start();
        new PeerPerformanceTestGarlicMessageJob().start();

        FPStoreManager.cleanUp();

    }
}
