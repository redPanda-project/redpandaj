package im.redpanda.transport;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.core.ServerContext;
import im.redpanda.identity.NodeId;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import org.junit.jupiter.api.Test;

/**
 * Second half of TD029 (REDPANDAJ-2EJ): {@code handleKeyWriteable()} dereferenced {@code
 * peer.writeBufferCrypted} without a null check, on the selector thread.
 *
 * <p>Locking the reaper in {@code PeerJobs} closes the race, but not this: a peer can reach a
 * writable key with its buffers already gone. {@link Peer#sendPing()} sets {@code connected=false}
 * when it finds a null {@code writeBuffer} (Peer.java:284-286) and leaves the {@link SelectionKey}
 * registered and valid, and the reaper then frees the pair of such a peer without cancelling that
 * key. Every other {@code writeBufferLock} section in the tree re-reads and null-checks these
 * fields; this one did not, and the NPE it threw was swallowed by the {@code catch (Exception)} in
 * {@code handleSelectionKey()}, which cancels the key — so the connection died either way, only
 * silently and with a Sentry event.
 */
class ConnectionHandlerWriteBufferGoneTest {

  @Test
  void writableKeyOfAPeerWithoutBuffersIsTornDownInsteadOfThrowing() {
    ByteBufferPool.init();
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    ConnectionHandler connectionHandler = new ConnectionHandler(serverContext, false);

    Peer peer = new Peer("46.224.156.238", 59558, NodeId.generateWithSimpleKey());
    peer.setConnected(true);
    // exactly the state PeerJobs' reaper leaves behind: no buffers, key still valid
    peer.writeBuffer = null;
    peer.writeBufferCrypted = null;

    SelectionKey key = new StubSelectionKey();
    key.attach(peer);
    peer.setSelectionKey(key);

    boolean handled = connectionHandler.handleKeyWriteable(key);

    assertThat(handled)
        .as("a peer whose write buffers are gone must be torn down, not dereferenced")
        .isTrue();
    assertThat(peer.isConnected()).isFalse();
  }

  /** Minimal valid {@link SelectionKey}: only {@code attach}/{@code isValid}/{@code cancel} run. */
  private static final class StubSelectionKey extends SelectionKey {
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
      return true;
    }

    @Override
    public void cancel() {
      // no-op
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
