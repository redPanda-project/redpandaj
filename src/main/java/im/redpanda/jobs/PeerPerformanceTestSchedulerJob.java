package im.redpanda.jobs;

import im.redpanda.core.Server;
import im.redpanda.core.ServerContext;
import java.util.concurrent.ThreadLocalRandom;

public class PeerPerformanceTestSchedulerJob extends Job {

  public PeerPerformanceTestSchedulerJob(ServerContext serverContext) {
    super(serverContext, 200L * 1L * 1L, true);
  }

  @Override
  public void init() {}

  @Override
  public void work() {

    if (Server.shuttingDown) {
      done();
      return;
    }

    new PeerPerformanceTestGarlicMessageJob(serverContext).start();

    if (PeerPerformanceTestGarlicMessageJob.getCountSuccess()
            + PeerPerformanceTestGarlicMessageJob.getCountFailed()
        < 5000) {
      setReRunDelay(100L);
    } else {
      setReRunDelay(2000L);
    }

    if (ThreadLocalRandom.current().nextDouble() < 0.01) {
      PeerPerformanceTestGarlicMessageJob.decayRates();
    }

    if (ThreadLocalRandom.current().nextDouble() < 0.01) {
      // lets randomly wait such that multiple edges are ready at once for more hops
      setReRunDelay(2000L);
    }
  }
}
