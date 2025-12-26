package im.redpanda.jobs;

import im.redpanda.core.Log;
import im.redpanda.core.Server;
import im.redpanda.core.ServerContext;

public class NodeStoreMaintainJob extends Job {

  public NodeStoreMaintainJob(ServerContext serverContext) {
    super(serverContext, 1000L * 5L * 1L, true, true);
  }

  @Override
  public void init() {
    // no need for job setup
  }

  @Override
  public void work() {

    try {
      if (serverContext.getNodeStore() != null && !Server.shuttingDown) {
        serverContext.getNodeStore().maintainNodes();
      }
    } catch (Exception e) {
      e.printStackTrace();
      Log.sentry(e);
    }
  }
}
