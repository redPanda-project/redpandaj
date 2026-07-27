package im.redpanda.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Regression tests for the accepted/readable socket leaks found in the 2026-07-26 bug hunt (H3 and
 * H6): every error path around an accepted connection has to release the socket, otherwise a peer
 * that connects and resets immediately (or a read burst that fills the bounded read queue) leaks
 * one file descriptor per occurrence.
 */
public class ConnectionHandlerSocketLeakTest {

  private ConnectionHandler handler;

  @Before
  public void setUp() {
    handler = new ConnectionHandler(ServerContext.buildDefaultServerContext(), false);
  }

  @After
  public void tearDown() {
    ConnectionHandler.peerInHandshakes.clear();
    ConnectionHandler.peersToReadAndParse.clear();
    ConnectionHandler.workingRead.clear();
  }

  /**
   * {@code ServerSocketChannel.accept()} returns null on a spurious selector wakeup. The old code
   * called {@code configureBlocking(false)} on the result before entering its try block, so the NPE
   * escaped keyAccept's own handler.
   */
  @Test
  public void keyAccept_ignoresNullAcceptFromSpuriousWakeup() throws Exception {
    try (ServerSocketChannel serverChannel = ServerSocketChannel.open()) {
      serverChannel.configureBlocking(false);
      serverChannel.bind(new InetSocketAddress("127.0.0.1", 0));

      SelectionKey key = serverChannel.register(ConnectionHandler.selector, SelectionKey.OP_ACCEPT);
      try {
        Method keyAccept =
            ConnectionHandler.class.getDeclaredMethod("keyAccept", SelectionKey.class);
        keyAccept.setAccessible(true);

        // nothing is connecting, so accept() returns null
        assertThatCode(() -> keyAccept.invoke(handler, key)).doesNotThrowAnyException();
      } finally {
        key.cancel();
      }
    }
  }

  /**
   * H3: an accepted channel of a peer that already reset the connection must be closed. An
   * unconnected channel reproduces that case — {@code socket().getInetAddress()} is null.
   */
  @Test
  public void setupAcceptedChannel_closesChannelWhenPeerIsAlreadyGone() throws Exception {
    SocketChannel channel = SocketChannel.open();
    try {
      int handshakesBefore = ConnectionHandler.peerInHandshakes.size();

      handler.setupAcceptedChannel(channel);

      assertThat(channel.isOpen()).isFalse();
      assertThat(ConnectionHandler.peerInHandshakes).hasSize(handshakesBefore);
    } finally {
      channel.close();
    }
  }

  /**
   * H3: the same has to hold when the setup throws — here the selector registration fails. The old
   * code only logged and left the accepted channel open.
   */
  @Test
  public void setupAcceptedChannel_closesChannelWhenSetupThrows() throws Exception {
    Selector originalSelector = ConnectionHandler.selector;
    Selector closedSelector = Selector.open();
    closedSelector.close();

    try (ServerSocketChannel serverChannel = ServerSocketChannel.open();
        SocketChannel client = SocketChannel.open()) {
      serverChannel.bind(new InetSocketAddress("127.0.0.1", 0));
      client.connect(serverChannel.getLocalAddress());

      SocketChannel accepted = serverChannel.accept();
      try {
        int handshakesBefore = ConnectionHandler.peerInHandshakes.size();
        ConnectionHandler.selector = closedSelector;

        handler.setupAcceptedChannel(accepted);

        assertThat(accepted.isOpen()).isFalse();
        assertThat(ConnectionHandler.peerInHandshakes).hasSize(handshakesBefore);
      } finally {
        ConnectionHandler.selector = originalSelector;
        accepted.close();
      }
    }
  }

  /**
   * H6: when the bounded read queue is full, the peer has to be disconnected. The old {@code add()}
   * threw IllegalStateException, which only reached handleSelectionKey's generic catch — that
   * cancels the key but leaves the socket open and the peer in the peerList.
   */
  @Test
  public void handleKeyReadable_disconnectsPeerWhenReadQueueIsFull() throws Exception {
    SocketChannel channel = SocketChannel.open();
    channel.configureBlocking(false);
    SelectionKey key = channel.register(ConnectionHandler.selector, SelectionKey.OP_READ);

    Peer peer = new Peer("127.0.0.1", 1234);
    peer.setSocketChannel(channel);
    peer.setSelectionKey(key);
    peer.setConnected(true);
    key.attach(peer);

    // Filling the real queue up to its capacity made this test flaky: peersToReadAndParse is
    // static and any ConnectionReaderThread left behind by an earlier test class keeps polling it,
    // so the queue could have room again by the time handleKeyReadable ran and the peer was never
    // disconnected. A queue that always rejects is the same condition for handleKeyReadable
    // (offer() == false / add() throwing IllegalStateException) but nothing can undo it.
    BlockingQueue<Peer> originalQueue = ConnectionHandler.peersToReadAndParse;
    ConnectionHandler.peersToReadAndParse =
        new LinkedBlockingQueue<>() {
          @Override
          public boolean offer(Peer peerToQueue) {
            return false;
          }
        };

    try {
      Method handleKeyReadable =
          ConnectionHandler.class.getDeclaredMethod("handleKeyReadable", SelectionKey.class);
      handleKeyReadable.setAccessible(true);

      handleKeyReadable.invoke(handler, key);
    } finally {
      ConnectionHandler.peersToReadAndParse = originalQueue;
    }

    assertThat(ConnectionHandler.workingRead).doesNotContain(peer);
    assertThat(peer.isConnected()).isFalse();
    assertThat(channel.isOpen()).isFalse();
  }
}
