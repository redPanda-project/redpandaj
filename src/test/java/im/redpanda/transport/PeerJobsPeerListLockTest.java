package im.redpanda.transport;

import static org.junit.jupiter.api.Assertions.assertTrue;

import im.redpanda.core.ServerContext;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import org.junit.jupiter.api.Test;

/**
 * Regression test for TD026: {@code PeerJobs.runOnce()} used to hold the {@link PeerList} read lock
 * across its whole per-peer loop — a loop that sleeps 20 ms per peer. With a few hundred known
 * peers that is several seconds of continuously held read lock, which starves every writer.
 *
 * <p>That became a user-visible latency regression once {@code PeerList.add()} started taking the
 * write lock unconditionally (#280): {@code ConnectionHandler.setupConnection()} calls {@code
 * add()} on the selector thread, so the entire NIO event loop — every read, every write, for every
 * peer — stalled for as long as this loop held the read lock. Measured peer ping went from ~20 ms
 * (the true network RTT) to hundreds of milliseconds and multi-second outliers.
 *
 * <p>The assertion is one-sided and not timing-flaky: the probe peer parks inside the loop until
 * the test thread has had its chance at the write lock, so a slow machine only gives the writer
 * more time to succeed, never less.
 */
class PeerJobsPeerListLockTest {

  /**
   * Generous upper bound for acquiring the write lock while {@code runOnce()} is mid-loop. With the
   * defect the lock is held for the entire loop, so this always times out; without it the lock is
   * free the instant the snapshot is taken.
   */
  private static final long WRITE_LOCK_TIMEOUT_MS = 2000;

  @Test
  void runOnce_doesNotHoldThePeerListReadLockWhileIteratingPeers() throws Exception {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    serverContext.setConnectionHandler(new ConnectionHandler(serverContext, false));
    ConnectionHandler.peerInHandshakes.clear();

    CountDownLatch insideLoop = new CountDownLatch(1);
    CountDownLatch writerDone = new CountDownLatch(1);

    // runOnce() sleeps, logs and evaluates the timeout condition before it reaches isConnected(),
    // so this is not the first call it makes on a peer — but it is inside the per-peer body, which
    // is all this test needs: parking here holds the loop at a point where the defective version
    // would still be holding the read lock.
    Peer probe =
        new Peer("127.0.0.1", 45001) {
          @Override
          public boolean isConnected() {
            insideLoop.countDown();
            try {
              writerDone.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            return false;
          }
        };
    serverContext.getPeerList().add(probe);

    PeerJobs peerJobs = new PeerJobs(serverContext);
    Thread jobThread = new Thread(peerJobs::runOnce, "peerjobs-under-test");
    jobThread.setDaemon(true);
    jobThread.start();

    assertTrue(insideLoop.await(30, TimeUnit.SECONDS), "runOnce() never reached the per-peer loop");

    Lock writeLock = serverContext.getPeerList().getReadWriteLock().writeLock();
    boolean acquired = writeLock.tryLock(WRITE_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    if (acquired) {
      writeLock.unlock();
    }

    writerDone.countDown();
    jobThread.join(TimeUnit.SECONDS.toMillis(30));

    assertTrue(
        acquired,
        "PeerJobs.runOnce() must not hold the peer list read lock while it iterates and sleeps —"
            + " it starves peerList.add() on the selector thread and stalls all peer I/O");
  }
}
