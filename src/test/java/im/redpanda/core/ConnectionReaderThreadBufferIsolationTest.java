package im.redpanda.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import im.redpanda.core.exceptions.PeerProtocolException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Test;

/**
 * Regression tests for TD009 (T50 review finding, fixed in T53): {@code myReaderBuffer} in {@link
 * ConnectionReaderThread} is a per-reader-thread scratch buffer reused across every {@link Peer}
 * this reader thread services over its lifetime (see the shared poll loop in {@link
 * ConnectionReaderThread#run()}). If it is ever left with leftover ciphertext bytes after a read
 * (position != 0), those bytes would be prefixed onto whatever peer this thread reads next and
 * decrypted under THAT peer's session keys — a cross-peer ciphertext-mixing window.
 *
 * <p>Two scenarios are covered:
 *
 * <ul>
 *   <li>{@link #corruptedFrameDoesNotLeakIntoNextPeersRead()} drives the one live path that used to
 *       leave {@code myReaderBuffer} dirty (a thrown {@link PeerProtocolException} from a bad GCM
 *       frame) through the real socket/decrypt code, and proves the SAME reader thread reading a
 *       second, unrelated peer afterwards is unaffected.
 *   <li>{@link #invariantGuardClearsBufferAndDisconnectsPeerOnDirectViolation()} drives {@link
 *       ConnectionReaderThread#assertReaderBufferReadyForNextRead} directly with a manually
 *       prepared dirty buffer (package-private field), since no live path is known to reach that
 *       guard any more after the fix above — see its javadoc for the analysis. This is the "prepare
 *       a buffer with leftover bytes" scenario asked for in T53.
 * </ul>
 */
public class ConnectionReaderThreadBufferIsolationTest {

  static {
    ByteBufferPool.init();
  }

  private final List<ServerSocketChannel> serverSockets = new ArrayList<>();
  private final List<SocketChannel> openChannels = new ArrayList<>();

  @After
  public void tearDownChannels() throws IOException {
    for (SocketChannel channel : openChannels) {
      if (channel.isOpen()) {
        channel.close();
      }
    }
    for (ServerSocketChannel serverSocket : serverSockets) {
      if (serverSocket.isOpen()) {
        serverSocket.close();
      }
    }
  }

  /** One loopback connection with its own, independent GCM session key. */
  private static final class PeerFixture {
    final Peer peer;
    final SocketChannel remoteSide;

    PeerFixture(Peer peer, SocketChannel remoteSide) {
      this.peer = peer;
      this.remoteSide = remoteSide;
    }
  }

  private PeerFixture newConnectedPeerFixture() throws IOException {
    ServerSocketChannel serverSocket = ServerSocketChannel.open();
    serverSocket.bind(new InetSocketAddress("127.0.0.1", 0));
    serverSockets.add(serverSocket);
    SocketChannel remoteSide = SocketChannel.open(serverSocket.getLocalAddress());
    SocketChannel peerSide = serverSocket.accept();
    openChannels.add(remoteSide);
    openChannels.add(peerSide);

    Peer peer = new Peer("127.0.0.1", 0, new NodeId());
    peer.setSocketChannel(peerSide);
    peer.setConnected(true);
    peer.writeBuffer = ByteBuffer.allocate(1024 * 100);
    peer.writeBufferCrypted = ByteBuffer.allocate(1024 * 100);
    // Each fixture gets its OWN random key, unlike the shared-key loopback trick elsewhere: the
    // whole point here is that two DIFFERENT peers must never decrypt under each other's keys.
    byte[] key = new byte[32];
    new java.security.SecureRandom().nextBytes(key);
    peer.setPeerChiperStreams(new GcmFramedStreams(key, key));

    return new PeerFixture(peer, remoteSide);
  }

  private ConnectionReaderThread newReaderThread() {
    return new ConnectionReaderThread(new ServerContext(), ConnectionReaderThread.STD_TIMEOUT);
  }

  private void sendEncrypted(PeerFixture fixture, byte[] plaintext) throws IOException {
    ByteBuffer in = ByteBuffer.allocate(plaintext.length);
    in.put(plaintext);
    in.flip();
    ByteBuffer out = ByteBuffer.allocate(plaintext.length + 1024);
    fixture.peer.getPeerChiperStreams().encrypt(in, out);
    out.flip();
    while (out.hasRemaining()) {
      fixture.remoteSide.write(out);
    }
  }

  /** Same frame as {@link #sendEncrypted}, but with the trailing GCM tag byte flipped. */
  private void sendCorruptedFrame(PeerFixture fixture, byte[] plaintext) throws IOException {
    ByteBuffer in = ByteBuffer.allocate(plaintext.length);
    in.put(plaintext);
    in.flip();
    ByteBuffer out = ByteBuffer.allocate(plaintext.length + 1024);
    fixture.peer.getPeerChiperStreams().encrypt(in, out);
    out.flip();
    byte[] frame = new byte[out.remaining()];
    out.get(frame);
    frame[frame.length - 1] ^= (byte) 0xFF; // corrupt the last GCM tag byte -> auth failure
    ByteBuffer corrupted = ByteBuffer.wrap(frame);
    while (corrupted.hasRemaining()) {
      fixture.remoteSide.write(corrupted);
    }
  }

  /**
   * The live path (before this fix): a bad GCM frame makes {@code decryptInputData} throw {@link
   * PeerProtocolException} after already draining {@code myReaderBuffer} into the framed cipher's
   * internal reassembly buffer, leaving it at position == limit != 0 instead of a clean state. The
   * SAME {@link ConnectionReaderThread} instance is then handed a second, unrelated peer with its
   * own independent key — it must read that peer's PONG cleanly, with no trace of the corrupted
   * first peer's bytes.
   */
  @Test
  public void corruptedFrameDoesNotLeakIntoNextPeersRead() throws Exception {
    ConnectionReaderThread readerThread = newReaderThread();

    PeerFixture peer1 = newConnectedPeerFixture();
    sendCorruptedFrame(peer1, new byte[] {Command.PONG});

    assertThatThrownBy(() -> readerThread.readConnection(peer1.peer))
        .isInstanceOf(PeerProtocolException.class);

    assertThat(readerThread.myReaderBuffer.position())
        .as("myReaderBuffer must be cleared right at the decrypt-failure throw site")
        .isZero();
    assertThat(readerThread.myReaderBuffer.limit())
        .isEqualTo(readerThread.myReaderBuffer.capacity());

    PeerFixture peer2 = newConnectedPeerFixture();
    sendEncrypted(peer2, new byte[] {Command.PONG});

    int read = readerThread.readConnection(peer2.peer);

    assertThat(read).isGreaterThan(0);
    assertThat(peer2.peer.lastCommand)
        .as("peer2 must parse its own PONG cleanly, unaffected by peer1's corrupted bytes")
        .isEqualTo(Command.PONG);
  }

  /**
   * Direct simulation of an endcheck violation ("buffer prepared with leftover bytes"): no live
   * path reaches {@link ConnectionReaderThread#assertReaderBufferReadyForNextRead} any more (see
   * its javadoc), so this drives the guard directly via the package-private {@code myReaderBuffer}
   * field, with position != 0 and limit == capacity — precisely the corner case the old, weaker
   * check (`pos != 0 && limit != capacity`) used to let through silently.
   */
  @Test
  public void invariantGuardClearsBufferAndDisconnectsPeerOnDirectViolation() throws Exception {
    ConnectionReaderThread readerThread = newReaderThread();
    PeerFixture peer1 = newConnectedPeerFixture();

    // Simulate leftover ciphertext from a hypothetical prior peer's read.
    readerThread.myReaderBuffer.put(new byte[] {1, 2, 3, 4, 5});
    assertThat(readerThread.myReaderBuffer.position()).isEqualTo(5);
    assertThat(readerThread.myReaderBuffer.limit())
        .as("the corner case the old check tolerated: limit == capacity despite pos != 0")
        .isEqualTo(readerThread.myReaderBuffer.capacity());

    assertThatThrownBy(() -> readerThread.assertReaderBufferReadyForNextRead(peer1.peer))
        .as("(a) the error path must fire")
        .isInstanceOf(PeerProtocolException.class)
        .hasMessageContaining("cleared buffer and disconnected peer");

    assertThat(peer1.peer.isConnected())
        .as("the peer whose bytes were stuck in the buffer must be disconnected")
        .isFalse();
    assertThat(readerThread.myReaderBuffer.position()).isZero();
    assertThat(readerThread.myReaderBuffer.limit())
        .isEqualTo(readerThread.myReaderBuffer.capacity());

    // (b) the next peer serviced by this same reader thread must never see the {1,2,3,4,5} bytes.
    PeerFixture peer2 = newConnectedPeerFixture();
    sendEncrypted(peer2, new byte[] {Command.PONG});

    int read = readerThread.readConnection(peer2.peer);

    assertThat(read).isGreaterThan(0);
    assertThat(peer2.peer.lastCommand)
        .as("peer2 must parse its own PONG cleanly, with no trace of the prepared leftover bytes")
        .isEqualTo(Command.PONG);
  }
}
