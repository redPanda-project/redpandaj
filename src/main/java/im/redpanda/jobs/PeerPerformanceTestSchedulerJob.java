package im.redpanda.jobs;

import im.redpanda.core.Peer;
import im.redpanda.core.Server;
import im.redpanda.core.ServerContext;

public class PeerPerformanceTestSchedulerJob extends Job {


    public PeerPerformanceTestSchedulerJob(ServerContext serverContext) {
        super(serverContext, 200L * 1L * 1L, true);
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


        if (PeerPerformanceTestGarlicMessageJob.getSuccessRate() < 0.9 && PeerPerformanceTestGarlicMessageJob.getCountSuccess() + PeerPerformanceTestGarlicMessageJob.getCountFailed() < 10000) {
            setReRunDelay(100L);
        } else {
            setReRunDelay(2000L);
        }

        if (Math.random() < 0.01) {
            PeerPerformanceTestGarlicMessageJob.decayRates();
        }

    }
}
