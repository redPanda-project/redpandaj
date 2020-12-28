package im.redpanda.jobs;

import im.redpanda.core.Peer;
import im.redpanda.core.PeerList;

public class PeerPerformanceTestSchedulerJob extends Job {


    public PeerPerformanceTestSchedulerJob() {
        super(1000L * 10L * 1L, true);
    }

    @Override
    public void init() {

    }

    @Override
    public void work() {

        System.out.println("fhsjfsdf");

        Peer goodPeer = PeerList.getGoodPeer();

        if (goodPeer == null) {
            return;
        }

        new PeerPerformanceTestFlaschenpostJob(goodPeer).start();

    }
}
