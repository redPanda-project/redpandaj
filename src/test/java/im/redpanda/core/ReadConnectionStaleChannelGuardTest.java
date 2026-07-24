package im.redpanda.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Collections;
import java.util.Set;
import org.junit.Test;

/**
 * Regression tests for TD012 (review-dag finding on redpandaj#271): the stale-connection guards in
 * {@link ConnectionReaderThread#readConnection(Peer)} — {@code peer.getSocketChannel() != channel},
 * checked in the {@code IOException}/{@code Throwable} catch blocks after a failed {@code
 * channel.read()} — exist to stop an I/O failure on an OLD, already-replaced channel (a concurrent
 * re-handshake via {@code Peer.setupConnectionForPeer}, e.g. IncomingHandler thread) from tearing
 * down the freshly established replacement connection. Every pre-existing test only ever triggers
 * this branch via {@code peer.setConnected(false)}, never with an actually swapped {@link
 * Peer#getSocketChannel()} instance — the exact identity comparison the guard performs.
 *
 * <p>{@link SocketChannel} and {@link SelectionKey} are abstract with no in-repo test stub, so this
 * drives the swap deterministically with two minimal purpose-built stubs below: {@link
 * SwapThenFailSocketChannel#read} performs the swap (mutates {@code peer.socketChannel} to a
 * different instance, simulating the concurrent re-handshake completing between the channel being
 * captured and the read failing) and then throws an {@link IOException} — precisely what reading a
 * killed/reused old fd looks like from the reader thread's side, at the moment the guard is meant
 * to catch it. {@link CancelTrackingSelectionKey} lets the tests observe that {@code key.cancel()}
 * (which unconditionally runs before the guard check, see {@code readConnection}) still happens
 * regardless of the swap.
 */
public class ReadConnectionStaleChannelGuardTest {

  static {
    ByteBufferPool.init();
  }

  private ConnectionReaderThread newReaderThread() {
    return new ConnectionReaderThread(new ServerContext(), ConnectionReaderThread.STD_TIMEOUT);
  }

  private Peer newConnectedPeer() {
    Peer peer = new Peer("127.0.0.1", 0, new NodeId());
    peer.setConnected(true);
    return peer;
  }

  /**
   * The channel captured at the top of readConnection() fails with an IOException, but only AFTER a
   * concurrent re-handshake has already swapped {@code peer.socketChannel} to a different (fresh)
   * instance — the exact race the guard defends against. The fresh connection must survive: no
   * {@code disconnect()}, peer stays connected, {@code socketChannel} stays the swapped-in
   * instance.
   */
  @Test
  public void staleReadFailureAfterConcurrentSwapDoesNotDisconnectFreshConnection()
      throws Exception {
    Peer peer = newConnectedPeer();
    CancelTrackingSelectionKey key = new CancelTrackingSelectionKey();
    peer.setSelectionKey(key);

    SocketChannel freshConnection = SocketChannel.open();
    try {
      SwapThenFailSocketChannel staleChannel = new SwapThenFailSocketChannel(peer, freshConnection);
      peer.setSocketChannel(staleChannel);

      int read = newReaderThread().readConnection(peer);

      assertThat(read).isEqualTo(0);
      assertThat(key.cancelled)
          .as("the stale key is cancelled unconditionally, before the guard is even evaluated")
          .isTrue();
      assertThat(peer.isConnected())
          .as(
              "the guard must prevent the stale read failure from tearing down the fresh"
                  + " (swapped-in) connection")
          .isTrue();
      assertThat(peer.getSocketChannel())
          .as("the swapped-in connection must be left completely untouched by the stale failure")
          .isSameAs(freshConnection);
    } finally {
      freshConnection.close();
    }
  }

  /**
   * Same read failure, but WITHOUT a concurrent swap: the channel captured at the top of
   * readConnection() is still the one and only connection the peer has. The guard must not suppress
   * a genuine disconnect in this case — this is the negative control proving the first test's green
   * result comes from the swap, not from the guard being a no-op.
   */
  @Test
  public void staleReadFailureWithoutSwapDisconnectsTheSingleConnection() throws Exception {
    Peer peer = newConnectedPeer();
    CancelTrackingSelectionKey key = new CancelTrackingSelectionKey();
    peer.setSelectionKey(key);

    SwapThenFailSocketChannel channel = new SwapThenFailSocketChannel(peer, null);
    peer.setSocketChannel(channel);

    int read = newReaderThread().readConnection(peer);

    assertThat(read).isEqualTo(0);
    assertThat(key.cancelled).isTrue();
    assertThat(peer.isConnected())
        .as("without a concurrent swap the guard must not suppress the genuine disconnect")
        .isFalse();
  }

  /**
   * On {@link #read(ByteBuffer)}, optionally swaps {@code peer.socketChannel} to a different
   * instance (simulating a concurrent re-handshake completing while this read is in flight), then
   * always fails with an {@link IOException}. Every other {@link SocketChannel} method is unused by
   * {@code readConnection()}'s failure path and throws {@link UnsupportedOperationException} if
   * ever called, so an unexpected code path fails loudly instead of silently doing nothing.
   */
  private static final class SwapThenFailSocketChannel extends SocketChannel {
    private final Peer peer;
    private final SocketChannel swapTo;

    SwapThenFailSocketChannel(Peer peer, SocketChannel swapTo) {
      super(SelectorProvider.provider());
      this.peer = peer;
      this.swapTo = swapTo;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
      if (swapTo != null) {
        peer.setSocketChannel(swapTo);
      }
      throw new IOException("simulated read failure on a stale connection (TD012)");
    }

    @Override
    protected void implCloseSelectableChannel() {
      // no-op: no real fd/selector registration to release
    }

    @Override
    protected void implConfigureBlocking(boolean block) {
      // no-op: Peer.disconnect() calls configureBlocking(false) unconditionally before close()
    }

    @Override
    public SocketChannel bind(SocketAddress local) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> SocketChannel setOption(SocketOption<T> name, T value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T getOption(SocketOption<T> name) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Set<SocketOption<?>> supportedOptions() {
      return Collections.emptySet();
    }

    @Override
    public SocketChannel shutdownInput() {
      throw new UnsupportedOperationException();
    }

    @Override
    public SocketChannel shutdownOutput() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Socket socket() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isConnected() {
      return true;
    }

    @Override
    public boolean isConnectionPending() {
      return false;
    }

    @Override
    public boolean connect(SocketAddress remote) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean finishConnect() {
      throw new UnsupportedOperationException();
    }

    @Override
    public SocketAddress getRemoteAddress() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int write(ByteBuffer src) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) {
      throw new UnsupportedOperationException();
    }

    @Override
    public SocketAddress getLocalAddress() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Minimal {@link SelectionKey} stub that only tracks whether {@link #cancel()} was called; {@code
   * readConnection()} calls {@code key.cancel()} unconditionally in the failure path, before the
   * stale-connection guard is even evaluated, so the tests need a key that survives that call
   * without an actual {@link Selector} registration.
   */
  private static final class CancelTrackingSelectionKey extends SelectionKey {
    private volatile boolean cancelled = false;

    @Override
    public SelectableChannel channel() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Selector selector() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isValid() {
      return !cancelled;
    }

    @Override
    public void cancel() {
      cancelled = true;
    }

    @Override
    public int interestOps() {
      throw new UnsupportedOperationException();
    }

    @Override
    public SelectionKey interestOps(int ops) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int readyOps() {
      throw new UnsupportedOperationException();
    }
  }
}
