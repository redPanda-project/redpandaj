package im.redpanda.jobs;

import im.redpanda.core.Peer;
import im.redpanda.core.Server;
import im.redpanda.core.ServerContext;

public class PeerPerformanceTestSchedulerJob extends Job {


    public PeerPerformanceTestSchedulerJob(ServerContext serverContext) {
        super(serverContext, 2000L * 1L * 1L, true);
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

        new PeerPerformanceTestGarlicMessageJob(serverContext).start();
//        FPStoreManager.cleanUp();

    }
}
