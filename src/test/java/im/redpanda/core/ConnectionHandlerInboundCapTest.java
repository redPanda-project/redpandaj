package im.redpanda.core;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.ops.Settings;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for the inbound accept budget (T66): {@code setupAcceptedChannel} must close a
 * freshly accepted channel once the selector's socket budget ({@link
 * Settings#MAX_INBOUND_CONNECTIONS}) is exhausted, and must keep accepting below it. The budget is
 * measured against {@code ConnectionHandler.selector.keys().size()}, so the tests derive their
 * thresholds from the current key count instead of assuming an empty selector — other test classes
 * share the static selector.
 */
class ConnectionHandlerInboundCapTest {

  private ConnectionHandler handler;
  private int originalMaxInbound;

  @BeforeEach
  void setUp() {
    ByteBufferPool.init();
    handler = new ConnectionHandler(ServerContext.buildDefaultServerContext(), false);
    originalMaxInbound = Settings.MAX_INBOUND_CONNECTIONS;
  }

  @AfterEach
  void tearDown() {
    Settings.MAX_INBOUND_CONNECTIONS = originalMaxInbound;
    ConnectionHandler.peerInHandshakes.clear();
  }

  /** At the budget, the accepted channel is closed and no handshake state is created. */
  @Test
  void setupAcceptedChannel_rejectsWhenSocketBudgetExhausted() throws Exception {
    try (ServerSocketChannel serverChannel = ServerSocketChannel.open();
        SocketChannel client = SocketChannel.open()) {
      serverChannel.bind(new InetSocketAddress("127.0.0.1", 0));
      client.connect(serverChannel.getLocalAddress());

      SocketChannel accepted = serverChannel.accept();
      try {
        int handshakesBefore = ConnectionHandler.peerInHandshakes.size();
        // the budget is already used up by whatever is registered right now
        Settings.MAX_INBOUND_CONNECTIONS = ConnectionHandler.selector.keys().size();

        handler.setupAcceptedChannel(accepted);

        assertThat(accepted.isOpen()).isFalse();
        assertThat(ConnectionHandler.peerInHandshakes).hasSize(handshakesBefore);
      } finally {
        accepted.close();
      }
    }
  }

  /** One below the budget, the same channel is accepted and enters the handshake. */
  @Test
  void setupAcceptedChannel_acceptsBelowSocketBudget() throws Exception {
    try (ServerSocketChannel serverChannel = ServerSocketChannel.open();
        SocketChannel client = SocketChannel.open()) {
      serverChannel.bind(new InetSocketAddress("127.0.0.1", 0));
      client.connect(serverChannel.getLocalAddress());

      SocketChannel accepted = serverChannel.accept();
      PeerInHandshake createdHandshake = null;
      try {
        int handshakesBefore = ConnectionHandler.peerInHandshakes.size();
        // exactly one accept still fits into the budget
        Settings.MAX_INBOUND_CONNECTIONS = ConnectionHandler.selector.keys().size() + 1;

        handler.setupAcceptedChannel(accepted);

        assertThat(accepted.isOpen()).isTrue();
        assertThat(ConnectionHandler.peerInHandshakes).hasSize(handshakesBefore + 1);
        createdHandshake = ConnectionHandler.peerInHandshakes.get(handshakesBefore);
      } finally {
        if (createdHandshake != null) {
          SelectionKey key = createdHandshake.getKey();
          if (key != null) {
            key.cancel();
          }
          handler.removePeerInHandshake(createdHandshake);
        }
        accepted.close();
      }
    }
  }
}
