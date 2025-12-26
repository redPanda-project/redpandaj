package im.redpanda.jobs;

import im.redpanda.core.Peer;
import im.redpanda.core.PeerList;
import im.redpanda.core.ServerContext;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;

public class KademliaPeerLookupJob extends Job {

  private final TreeMap<Peer, Integer> peers = new TreeMap<>();

  public KademliaPeerLookupJob(ServerContext serverContext) {
    super(serverContext);

    PeerList peerList = serverContext.getPeerList();

    // insert all nodes
    Lock lock = peerList.getReadWriteLock().readLock();
    lock.lock();
    try {
      ArrayList<Peer> peerArrayList = peerList.getPeerArrayList();

      for (Peer p : peerArrayList) {
        peers.put(p, 0);
      }

    } finally {
      lock.unlock();
    }
  }

  @Override
  public void init() {}

  @Override
  public void work() {

    for (Peer p : peers.keySet()) {}
  }
}
