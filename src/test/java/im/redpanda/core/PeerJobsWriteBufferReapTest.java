package im.redpanda.core;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.identity.NodeId;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Regression test for TD029 (REDPANDAJ-2EJ), seen on a public seed node 76 s after the deploy of
 * 226ab3a: {@code NullPointerException: Cannot invoke "java.nio.ByteBuffer.flip()" because
 * "peer.writeBufferCrypted" is null} on the NIO selector thread.
 *
 * <p>{@code PeerJobs.runOnce()} frees the write buffers of a long-silent peer. It did so without
 * holding {@link Peer#writeBufferLock}, the lock that owns both fields everywhere else — {@link
 * Peer#disconnect(String)}, {@link Peer#setupConnectionForPeer(PeerInHandshake)} and {@code
 * ConnectionHandler.handleKeyWriteable()}. The selector reads {@code writeBufferCrypted} inside its
 * own {@code writeBufferLock} section, so the reaper could null it between the acquisition and the
 * deref.
 *
 * <p>The assertion is one-sided and therefore not timing-flaky (see {@link
 * ConcurrencyTestSupport}): a slow machine only gives the reaper more time to do the thing it must
 * not do.
 */
class PeerJobsWriteBufferReapTest {

  @AfterEach
  void tearDown() {
    ConnectionHandler.peerInHandshakes.clear();
  }

  private ServerContext context() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    serverContext.setConnectionHandler(new ConnectionHandler(serverContext, false));
    ConnectionHandler.peerInHandshakes.clear();
    return serverContext;
  }

  /**
   * A dialable peer that is neither connected nor connecting and never answered — i.e. exactly the
   * {@code lastAnswered > pingTimeout * 2} branch, and dialable so the T86 eviction keeps it.
   */
  private static Peer silentPeer() {
    Peer peer = new Peer("46.224.156.238", 59558, NodeId.generateWithSimpleKey());
    peer.writeBuffer = ByteBuffer.allocate(1024);
    peer.writeBufferCrypted = ByteBuffer.allocate(1024);
    return peer;
  }

  @Test
  void runOnce_freesTheWriteBuffersOnlyUnderTheWriteBufferLock() throws Exception {
    ServerContext ctx = context();
    Peer peer = silentPeer();
    ctx.getPeerList().add(peer);
    PeerJobs peerJobs = new PeerJobs(ctx);

    // Holding writeBufferLock is what ConnectionHandler.handleKeyWriteable() does for the whole
    // encrypt/flip/write section. While it is held, nothing may take these buffers away.
    ConcurrencyTestSupport.assertBlockedWhileHeld(peer.getWriteBufferLock(), peerJobs::runOnce);

    // ...and once the lock is free the reap must actually happen, so the test above cannot pass
    // vacuously by blocking somewhere else.
    assertThat(peer.writeBuffer).as("the reap must still run once the lock is released").isNull();
    assertThat(peer.writeBufferCrypted).isNull();
  }

  /**
   * {@code setupConnectionForPeer()} holds {@code writeBufferLock} across the entire connection
   * swap, so a reaper that decided to free the buffers can end up queued behind it and acquire the
   * lock only after the peer has reconnected and allocated fresh ones. Freeing those would tear
   * down a live connection, so the condition is re-tested under the lock.
   */
  @Test
  void runOnce_doesNotFreeTheBuffersOfAPeerThatReconnectedInTheMeantime() {
    ServerContext ctx = context();
    Peer peer =
        new Peer("46.224.156.238", 59558, NodeId.generateWithSimpleKey()) {
          @Override
          public boolean isConnected() {
            // The loop's own checks run before the lock is taken and must see the stale state
            // that makes runOnce() decide to reap; the check inside the lock sees the reconnect.
            return getWriteBufferLock().isHeldByCurrentThread();
          }
        };
    peer.writeBuffer = ByteBuffer.allocate(1024);
    peer.writeBufferCrypted = ByteBuffer.allocate(1024);
    ctx.getPeerList().add(peer);

    new PeerJobs(ctx).runOnce();

    assertThat(peer.writeBuffer)
        .as("a peer that reconnected while the reaper waited for the lock must keep its buffers")
        .isNotNull();
    assertThat(peer.writeBufferCrypted).isNotNull();
  }
}
