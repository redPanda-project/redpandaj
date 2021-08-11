package im.redpanda.jobs;

import im.redpanda.core.Peer;
import im.redpanda.core.Server;
import im.redpanda.core.ServerContext;

public class PeerPerformanceTestSchedulerJob extends Job {


    public PeerPerformanceTestSchedulerJob(ServerContext serverContext) {
        super(serverContext, 500L * 1L * 1L, true);
    }

    @Override
    public void init() {

    }

    @Override
    public void work() {

        if (Server.SHUTDOWN) {
            done();
            return;
        }

        Peer goodPeer = serverContext.getPeerList().getGoodPeer(0.5f); //todo change later if network is big enough

        if (goodPeer == null) {
            return;
        }

//        new PeerPerformanceTestFlaschenpostJob(goodPeer).start();
        new PeerPerformanceTestGarlicMessageJob(serverContext).start();

//        FPStoreManager.cleanUp();

    }
}
