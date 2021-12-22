package im.redpanda.jobs;

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

        if (Server.shuttingDown) {
            done();
            return;
        }

        new PeerPerformanceTestGarlicMessageJob(serverContext).start();


        if (PeerPerformanceTestGarlicMessageJob.getCountSuccess() + PeerPerformanceTestGarlicMessageJob.getCountFailed() < 5000) {
            setReRunDelay(100L);
        } else {
            setReRunDelay(2000L);
        }

        if (Math.random() < 0.01) {
            PeerPerformanceTestGarlicMessageJob.decayRates();
        }

    }
}
