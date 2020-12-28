package im.redpanda.jobs;

import im.redpanda.core.NodeId;
import im.redpanda.core.PeerList;
import im.redpanda.core.Server;
import im.redpanda.flaschenpost.GMAck;
import im.redpanda.flaschenpost.GMContent;
import im.redpanda.flaschenpost.GMParser;
import im.redpanda.flaschenpost.GarlicMessage;
import im.redpanda.kademlia.KadStoreManager;

import java.nio.ByteBuffer;

public class PeerPerformanceTestSchedulerJob extends Job {


    public PeerPerformanceTestSchedulerJob() {
        super(1000L * 10L * 1L, true);
    }

    @Override
    public void init() {

    }

    @Override
    public void work() {

        new PeerPerformanceTestFlaschenpostJob(PeerList.getGoodPeer()).start();

    }
}
