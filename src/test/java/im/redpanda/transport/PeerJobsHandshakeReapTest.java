package im.redpanda.transport;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.core.ServerContext;
import java.lang.reflect.Field;
import java.nio.channels.SocketChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Regression test for T68 (b): {@code PeerJobs} skipped the stale-{@link PeerInHandshake} cleanup
 * with {@code if (peerList.size() == 0) continue;} before it ran, so handshake channels were never
 * reaped while the peer list was empty — a fresh node or one that just lost every connection is
 * exactly the situation in which half-open handshakes pile up.
 */
class PeerJobsHandshakeReapTest {

  @AfterEach
  void tearDown() {
    ConnectionHandler.peerInHandshakes.clear();
  }

  @Test
  void runOnce_reapsStaleHandshakesWhilePeerListIsEmpty() throws Exception {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    serverContext.setConnectionHandler(new ConnectionHandler(serverContext, false));
    ConnectionHandler.peerInHandshakes.clear();

    assertThat(serverContext.getPeerList().size()).isZero();

    try (SocketChannel channel = SocketChannel.open()) {
      PeerInHandshake stale =
          new PeerInHandshake("127.0.0.1", new Peer("127.0.0.1", 1234), channel);
      ageOut(stale);
      serverContext.getConnectionHandler().addPeerInHandshake(stale);

      new PeerJobs(serverContext).runOnce();

      assertThat(channel.isOpen())
          .as("the stale handshake channel must be closed even with an empty peer list")
          .isFalse();
      assertThat(ConnectionHandler.peerInHandshakes).doesNotContain(stale);
    }
  }

  @Test
  void runOnce_keepsFreshHandshakes() throws Exception {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    serverContext.setConnectionHandler(new ConnectionHandler(serverContext, false));
    ConnectionHandler.peerInHandshakes.clear();

    try (SocketChannel channel = SocketChannel.open()) {
      PeerInHandshake fresh =
          new PeerInHandshake("127.0.0.1", new Peer("127.0.0.1", 1234), channel);
      serverContext.getConnectionHandler().addPeerInHandshake(fresh);

      new PeerJobs(serverContext).runOnce();

      assertThat(channel.isOpen()).isTrue();
      assertThat(ConnectionHandler.peerInHandshakes).contains(fresh);
    }
  }

  /** Backdates the handshake past {@link PeerJobs#HANDSHAKE_TIMEOUT_MS}. */
  private static void ageOut(PeerInHandshake peerInHandshake) throws Exception {
    Field createdAt = PeerInHandshake.class.getDeclaredField("createdAt");
    createdAt.setAccessible(true);
    createdAt.setLong(
        peerInHandshake, System.currentTimeMillis() - PeerJobs.HANDSHAKE_TIMEOUT_MS - 1000L);
  }
}
