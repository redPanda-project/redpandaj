package im.redpanda.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import im.redpanda.core.ServerContext;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConnectionHandlerTest {

  private ServerContext serverContext;
  private Set<ServerSocketChannel> channelsToClose;

  @BeforeEach
  void setUp() {
    serverContext = ServerContext.buildDefaultServerContext();
    channelsToClose = new HashSet<>();
  }

  @AfterEach
  void tearDown() throws Exception {
    // Clean up any channels we registered with the shared selector so other tests are unaffected.
    for (SelectionKey key : ConnectionHandler.selector.keys()) {
      if (key.channel() instanceof ServerSocketChannel) {
        key.cancel();
      }
    }
    for (ServerSocketChannel channel : channelsToClose) {
      channel.close();
    }

    ConnectionHandler.peerInHandshakes.clear();
  }

  @Test
  void bindToNextAvailablePortSkipsOccupiedPort() throws Exception {
    int occupiedPort;
    try (ServerSocket occupied = new ServerSocket(0)) {
      occupiedPort = occupied.getLocalPort();

      try (ServerSocketChannel channel = ServerSocketChannel.open()) {
        channel.configureBlocking(false);

        ConnectionHandler handler = new ConnectionHandler(serverContext, false);

        Method method =
            ConnectionHandler.class.getDeclaredMethod(
                "bindToNextAvailablePort", int.class, ServerSocketChannel.class);
        method.setAccessible(true);

        int boundPort = (int) method.invoke(handler, occupiedPort, channel);

        assertNotEquals(occupiedPort, boundPort, "should skip the occupied port");
        assertEquals(
            boundPort, channel.socket().getLocalPort(), "channel should be bound to returned port");
      }
    }
  }

  @Test
  void addAndRemovePeerInHandshakeUpdatesCollection() throws Exception {
    ConnectionHandler handler = new ConnectionHandler(serverContext, false);

    try (SocketChannel socketChannel = SocketChannel.open()) {
      PeerInHandshake peer = new PeerInHandshake("127.0.0.1", socketChannel);

      int before = ConnectionHandler.peerInHandshakes.size();
      handler.addPeerInHandshake(peer);
      assertTrue(ConnectionHandler.peerInHandshakes.contains(peer));
      assertEquals(before + 1, ConnectionHandler.peerInHandshakes.size());

      handler.removePeerInHandshake(peer);
      assertFalse(ConnectionHandler.peerInHandshakes.contains(peer));
      assertEquals(before, ConnectionHandler.peerInHandshakes.size());
    }
  }

  @Test
  void addServerSocketChannelRegistersForAccept() throws Exception {
    ConnectionHandler handler = new ConnectionHandler(serverContext, false);

    try (ServerSocketChannel channel = ServerSocketChannel.open()) {
      channel.configureBlocking(false);
      channel.bind(new InetSocketAddress(0));

      handler.addServerSocketChannel(channel);
      channelsToClose.add(channel);

      boolean found = false;
      for (SelectionKey key : ConnectionHandler.selector.keys()) {
        if (key.channel() == channel) {
          found = true;
          assertNotEquals(0, key.interestOps() & SelectionKey.OP_ACCEPT);
          break;
        }
      }
      assertTrue(found, "channel should be registered with selector");
    }
  }
}
