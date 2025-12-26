package im.redpanda.jobs;

import im.redpanda.core.Peer;
import im.redpanda.core.ServerContext;
import java.time.Duration;
import java.util.concurrent.locks.Lock;

public class NodeConnectionPointsSeenJob extends Job {

  public NodeConnectionPointsSeenJob(ServerContext serverContext) {
    super(serverContext, Duration.ofMinutes(2).toMillis(), true, true);
  }

  @Override
  public void init() {
    // nothing to do
  }

  @Override
  public void work() {

    Lock lock = serverContext.getPeerList().getReadWriteLock().readLock();
    lock.lock();
    try {
      for (Peer peer : serverContext.getPeerList().getPeerArrayList()) {
        if (peer.isConnected() && peer.getNode() != null) {
          peer.getNode().seen(peer.getIp(), peer.getPort());
        }
      }
    } finally {
      lock.unlock();
    }
  }
}
