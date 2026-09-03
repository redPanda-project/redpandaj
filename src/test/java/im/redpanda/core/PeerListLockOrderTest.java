package im.redpanda.core;

import static org.junit.jupiter.api.Assertions.assertTrue;

import im.redpanda.identity.NodeId;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Regression test for T87: the deadlock that took a public seed node off the network on 2026-07-29.
 *
 * <p>Two lock acquisition orders existed for the same pair of locks:
 *
 * <ul>
 *   <li>{@code ConnectionHandler.setupConnection()} — on the <b>NIO selector thread</b> — holds
 *       {@code peerOrigin.writeBufferLock} (ConnectionHandler.java:867) and then takes the peer
 *       list <b>write</b> lock via {@code peerList.add()} (ConnectionHandler.java:881).
 *   <li>{@code InboundCommandProcessor.handleRequestPeerList()} — on a reader thread — held the
 *       peer list <b>read</b> lock (InboundCommandProcessor.java:502) and then took that same
 *       peer's {@code writeBufferLock} (InboundCommandProcessor.java:529).
 * </ul>
 *
 * <p>Classic ABBA. Two extra properties turned it from "one stuck connection" into a dead node: the
 * {@code ReentrantReadWriteLock} is non-fair, so the selector's queued write request blocked every
 * later reader too, and the blocked thread is the selector — the one that calls {@code accept()}.
 * The node stayed alive with a full listen backlog ({@code ss -ltn} showed {@code LISTEN 51 50})
 * and no journal output.
 *
 * <p>The test reproduces exactly that interleaving with the real command handler. The peer list
 * lock is only ever released by the fix, never by timing, so this deadlocks (and fails on the
 * timeout) without it.
 */
class PeerListLockOrderTest {

  /**
   * Generous upper bound for the selector's {@code peerList.add()}. Without the fix the read lock
   * is held until the handler finishes, and the handler cannot finish, so no bound helps; with it
   * the lock is free the moment the snapshot is taken.
   */
  private static final long SELECTOR_LOCK_TIMEOUT_MS = 5000;

  private ServerContext ctx;
  private InboundCommandProcessor proc;

  @BeforeEach
  void setup() {
    ctx = ServerContext.buildDefaultServerContext();
    ctx.setPort(59558);
    proc = new InboundCommandProcessor(ctx);
    ByteBufferPool.init();
  }

  @Test
  void requestPeerList_doesNotHoldThePeerListLockWhileTakingTheWriteBufferLock() throws Exception {
    Peer requester = connectedPeer("84.147.60.253", 59558);
    requester.writeBuffer = ByteBuffer.allocate(1024 * 64);
    ctx.getPeerList().add(requester);
    ctx.getPeerList().add(connectedPeer("46.224.156.238", 59558));

    // The selector thread's half of the inversion: setupConnection() holds this peer's
    // writeBufferLock across its whole body. Held from the test thread here so the reader below
    // must queue on it, which is the state the seed node was in.
    requester.getWriteBufferLock().lock();
    CountDownLatch handlerDone = new CountDownLatch(1);
    AtomicReference<Throwable> handlerFailure = new AtomicReference<>();
    Thread reader =
        new Thread(
            () -> {
              // countDown() in a finally, and the throwable kept: without this a failure inside
              // parseCommand() would surface as a 30 s latch timeout instead of the real cause —
              // exactly the diagnosis this test exists to provide.
              try {
                proc.parseCommand(Command.REQUEST_PEERLIST, ByteBuffer.allocate(0), requester);
              } catch (Throwable t) {
                handlerFailure.set(t);
              } finally {
                handlerDone.countDown();
              }
            },
            "reader-under-test");
    reader.setDaemon(true);

    Lock peerListWriteLock = ctx.getPeerList().getReadWriteLock().writeLock();
    boolean selectorGotTheLock;
    try {
      reader.start();

      // Wait until the handler is genuinely parked on the writeBufferLock. Only then is the
      // question below meaningful: with the defect it is parked there holding the peer list read
      // lock, without it that lock was released before the buffer was touched. Polling the queue
      // makes this deterministic rather than a sleep-and-hope race.
      assertTrue(
          awaitQueuedOn(requester.getWriteBufferLock(), handlerDone),
          "REQUEST_PEERLIST handler never reached the write buffer");

      // The selector thread's next step: peerList.add(). This is what wedged the node.
      selectorGotTheLock =
          peerListWriteLock.tryLock(SELECTOR_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      if (selectorGotTheLock) {
        peerListWriteLock.unlock();
      }
    } finally {
      requester.getWriteBufferLock().unlock();
    }

    assertTrue(handlerDone.await(30, TimeUnit.SECONDS), "REQUEST_PEERLIST completed");
    if (handlerFailure.get() != null) {
      throw new AssertionError("REQUEST_PEERLIST handler threw", handlerFailure.get());
    }
    assertTrue(
        selectorGotTheLock,
        "handleRequestPeerList() must not hold the peer list lock while it waits for a peer's"
            + " writeBufferLock — ConnectionHandler.setupConnection() takes those two the other way"
            + " round on the selector thread, and the resulting deadlock stops accept() (T87)");
  }

  /**
   * Waits until some thread is queued on {@code lock}, i.e. blocked trying to acquire it. Gives up
   * early if the handler finished (or died) without ever getting there, so a broken handler fails
   * fast instead of burning the full timeout.
   */
  private static boolean awaitQueuedOn(
      java.util.concurrent.locks.ReentrantLock lock, CountDownLatch handlerDone)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
    while (System.currentTimeMillis() < deadline) {
      if (lock.hasQueuedThreads()) {
        return true;
      }
      if (handlerDone.getCount() == 0) {
        return false;
      }
      Thread.sleep(5);
    }
    return false;
  }

  private Peer connectedPeer(String ip, int port) {
    Peer peer = new Peer(ip, port, NodeId.generateWithSimpleKey());
    // no SelectionKey in a unit test, so setWriteBufferFilled() must stay a no-op
    peer.setConnected(false);
    return peer;
  }
}
