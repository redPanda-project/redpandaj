package im.redpanda.routing.graph;

import im.redpanda.core.Server;
import im.redpanda.core.ServerContext;
import im.redpanda.ops.Job;
import im.redpanda.ops.Log;

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
      if (serverContext.getNodeStore() != null && !Server.isShuttingDown()) {
        serverContext.getNodeStore().maintainNodes();
      }
    } catch (Exception e) {
      // Log.sentry already logs the throwable; the extra printStackTrace only duplicated it on
      // stdout, outside log4j's file appender.
      Log.sentry(e);
    }
  }
}
