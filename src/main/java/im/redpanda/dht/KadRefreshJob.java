package im.redpanda.dht;

import im.redpanda.core.ServerContext;
import im.redpanda.ops.Job;

public class KadRefreshJob extends Job {

  public KadRefreshJob(ServerContext serverContext) {
    super(serverContext, 1000L * 60L * 60L * 1L, true);
  }

  @Override
  public void init() {}

  @Override
  public void work() {

    System.out.println("refresh the KadContent");
    KadStoreManager.maintain(serverContext);
  }
}
