package im.redpanda.routing;

import im.redpanda.core.ServerContext;
import im.redpanda.ops.Job;

public class GMManagerCleanJobs extends Job {

  public GMManagerCleanJobs(ServerContext serverContext) {
    super(serverContext, 1000L * 60L * 5L, true);
  }

  @Override
  public void init() {
    // no need for job setup
  }

  @Override
  public void work() {
    GMStoreManager.cleanUp();
  }
}
