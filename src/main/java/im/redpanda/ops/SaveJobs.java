package im.redpanda.ops;

import im.redpanda.core.ServerContext;
import im.redpanda.transport.Saver;

public class SaveJobs extends Job {

  public SaveJobs(ServerContext serverContext) {
    super(serverContext, 1000L * 60L * 15L, true);
  }

  @Override
  public void init() {
    // no need for job setup
  }

  @Override
  public void work() {
    serverContext.getLocalSettings().save(serverContext.getPort());
    serverContext.getNodeStore().saveToDisk();
    Saver.savePeers(serverContext.getPeerList());
  }
}
