package im.redpanda.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Regression tests for the T50 / REDPANDAJ-2EF readBuffer ownership handoff in {@link
 * ConnectionReaderThread#readConnection(Peer)}: the reader claims {@code peer.readBuffer} under
 * {@code writeBufferLock} (field set to {@code null}) before parsing, so a concurrent {@link
 * Peer#disconnect(String)} — e.g. triggered by {@link Peer#setupConnectionForPeer} during a
 * re-handshake of an existing peer on the IncomingHandler thread — can no longer return the buffer
 * to the {@link ByteBufferPool} while the reader is still flipping/compacting it ("borrowObject
 * found an invalid ByteBuffer", cross-connection buffer reuse).
 *
 * <p>The tests drive the real {@code readConnection} code path over a loopback {@link
 * SocketChannel} pair with real {@link GcmFramedStreams} (same key for both directions, so the test
 * can encrypt frames the peer's receive stream will decrypt — same trick as {@link PeerTest}).
 *
 * <p>A fully concurrent interleaving test (thread A parked inside loopCommands while thread B runs
 * setupConnectionForPeer) is intentionally not attempted: {@code ConnectionReaderThread} creates
 * its {@code InboundCommandProcessor} internally and the command-handler table is private, so there
 * is no seam to deterministically park the parse loop without adding test hooks to production code.
 * Instead these tests pin every state transition of the claim/restore protocol (drain, restore,
 * disconnect-while-claimed, stale-connection drop) deterministically.
 */
public class ReadConnectionOwnershipHandoffTest {

  static {
    ByteBufferPool.init();
  }

  private ServerSocketChannel serverSocket;
  private SocketChannel remoteSide;
  private SocketChannel peerSide;

  @Before
  public void setUpChannels() throws IOException {
    serverSocket = ServerSocketChannel.open();
    serverSocket.bind(new InetSocketAddress("127.0.0.1", 0));
    remoteSide = SocketChannel.open(serverSocket.getLocalAddress());
    peerSide = serverSocket.accept();
  }

  @After
  public void tearDownChannels() throws IOException {
    for (SocketChannel channel : new SocketChannel[] {remoteSide, peerSide}) {
      if (channel != null && channel.isOpen()) {
        channel.close();
      }
    }
    if (serverSocket != null && serverSocket.isOpen()) {
      serverSocket.close();
    }
  }

  private Peer newConnectedPeer() {
    Peer peer = new Peer("127.0.0.1", 0, new NodeId());
    peer.setSocketChannel(peerSide);
    peer.setConnected(true);
    peer.writeBuffer = ByteBuffer.allocate(1024 * 100);
    peer.writeBufferCrypted = ByteBuffer.allocate(1024 * 100);
    // Same 32-byte key for both directions so the peer's receive stream decrypts frames encrypted
    // by its own send stream (loopback test trick, see PeerTest).
    byte[] key = new byte[32];
    peer.setPeerChiperStreams(new GcmFramedStreams(key, key));
    return peer;
  }

  private ConnectionReaderThread newReaderThread() {
    return new ConnectionReaderThread(new ServerContext(), ConnectionReaderThread.STD_TIMEOUT);
  }

  /** Encrypts the given plaintext with the peer's (loopback) streams and sends it to the peer. */
  private void sendEncrypted(Peer peer, byte[] plaintext) throws IOException {
    ByteBuffer in = ByteBuffer.allocate(plaintext.length);
    in.put(plaintext);
    in.flip();
    ByteBuffer out = ByteBuffer.allocate(plaintext.length + 1024);
    peer.getPeerChiperStreams().encrypt(in, out);
    out.flip();
    while (out.hasRemaining()) {
      remoteSide.write(out);
    }
  }

  /**
   * Fully drained buffer: after parsing, the claimed buffer must be returned to the pool and the
   * field must stay {@code null}. The LIFO pool handing back the very same instance on the next
   * borrow proves the return actually happened (and happened exactly once — a double return would
   * throw an IllegalStateException out of readConnection).
   */
  @Test
  public void drainedBufferIsReturnedToPoolAndFieldStaysNull() throws Exception {
    Peer peer = newConnectedPeer();
    ByteBuffer preBorrowed = ByteBufferPool.borrowObject(1024);
    peer.readBuffer = preBorrowed;

    sendEncrypted(peer, new byte[] {Command.PONG, Command.PONG, Command.PONG});

    int read = newReaderThread().readConnection(peer);

    assertThat(read).isGreaterThan(0);
    assertThat(peer.lastCommand).isEqualTo(Command.PONG);
    assertThat(peer.readBuffer)
        .as("a fully drained buffer must be returned to the pool, not kept in the field")
        .isNull();
    // LIFO pool: the instance we just returned is the first one handed out again, in pristine
    // state. This asserts the buffer really went back to the pool exactly once.
    ByteBuffer reBorrowed = ByteBufferPool.borrowObject(1024);
    assertThat(reBorrowed).isSameAs(preBorrowed);
    assertThat(reBorrowed.position()).isZero();
    assertThat(reBorrowed.limit()).isEqualTo(reBorrowed.capacity());
    ByteBufferPool.returnObject(reBorrowed);
  }

  /**
   * Unparsed rest bytes of a half-received command: the claimed buffer must be restored into {@code
   * peer.readBuffer} (same instance, bytes preserved) so the next read can complete the command.
   */
  @Test
  public void bufferWithLeftoverBytesIsRestoredIntoField() throws Exception {
    Peer peer = newConnectedPeer();
    ByteBuffer preBorrowed = ByteBufferPool.borrowObject(1024);
    peer.readBuffer = preBorrowed;

    // SEND_PEERLIST announces a 100-byte payload that never arrives -> parseCommand returns 0,
    // the command byte + length header (5 bytes) remain unparsed in the buffer.
    byte[] partialCommand = new byte[5];
    ByteBuffer header = ByteBuffer.wrap(partialCommand);
    header.put(Command.SEND_PEERLIST);
    header.putInt(100);
    sendEncrypted(peer, partialCommand);

    int read = newReaderThread().readConnection(peer);

    assertThat(read).isGreaterThan(0);
    assertThat(peer.readBuffer)
        .as("leftover bytes of a half command must be restored into the field")
        .isSameAs(preBorrowed);
    assertThat(peer.readBuffer.position())
        .as("the 5 header bytes must still be buffered (write mode)")
        .isEqualTo(5);
    assertThat(peer.isConnected()).isTrue();

    // cleanup: hand the buffer back via the production path
    peer.disconnect("test cleanup");
    assertThat(peer.readBuffer).isNull();
  }

  /**
   * Handler disconnects the peer mid-parse (here: unknown command byte, REDPANDAJ-2E0 behavior)
   * while the buffer is claimed: {@link Peer#disconnect(String)} must not return the claimed buffer
   * (the field is {@code null}), and afterwards readConnection itself must return it exactly once —
   * the pre-2EF code raced exactly here. A double return would throw an IllegalStateException from
   * the pool and fail this test.
   */
  @Test
  public void disconnectWhileBufferClaimedReturnsBufferExactlyOnce() throws Exception {
    Peer peer = newConnectedPeer();
    ByteBuffer preBorrowed = ByteBufferPool.borrowObject(1024);
    peer.readBuffer = preBorrowed;

    sendEncrypted(peer, new byte[] {0}); // unknown command -> handler disconnects the peer

    int read = newReaderThread().readConnection(peer);

    assertThat(read).isGreaterThan(0);
    assertThat(peer.isConnected()).isFalse();
    assertThat(peer.readBuffer)
        .as("after a mid-parse disconnect nothing may be left in the field")
        .isNull();
    // the reader (not disconnect) must have returned the claimed buffer to the pool, valid
    ByteBuffer reBorrowed = ByteBufferPool.borrowObject(1024);
    assertThat(reBorrowed).isSameAs(preBorrowed);
    assertThat(reBorrowed.position()).isZero();
    ByteBufferPool.returnObject(reBorrowed);
  }

  /**
   * Connection torn down between the socket read and the decrypt (the re-handshake window): the
   * stale ciphertext bytes must be dropped — no decrypt (which would desync the GCM frame nonce
   * counter of a replacement connection, REDPANDAJ-2EE), no borrow, no parse.
   */
  @Test
  public void staleBytesAfterDisconnectAreDroppedWithoutDecrypt() throws Exception {
    Peer peer = newConnectedPeer();

    sendEncrypted(peer, new byte[] {Command.PONG});
    // simulate the connection being replaced/torn down after the bytes were read off the socket
    peer.setConnected(false);

    int read = newReaderThread().readConnection(peer);

    assertThat(read).isGreaterThan(0);
    assertThat(peer.readBuffer).as("no buffer may be borrowed for dropped stale bytes").isNull();
    assertThat(peer.lastCommand).as("stale bytes must not be parsed").isEqualTo((byte) 0);
  }

  /**
   * Contract of {@link Peer#disconnect(String)} under the handoff: it only returns what is stored
   * in the field. A buffer claimed by a reader (field {@code null}) is left completely untouched —
   * no reset, no pool return.
   */
  @Test
  public void disconnectDoesNotTouchClaimedBuffer() {
    Peer peer = new Peer("127.0.0.1", 0, new NodeId());
    peer.setConnected(true);

    ByteBuffer claimed = ByteBufferPool.borrowObject(1024);
    claimed.put((byte) 42); // reader is mid-parse, position 1
    peer.readBuffer = null; // claimed: field is empty

    peer.disconnect("test");

    assertThat(peer.isConnected()).isFalse();
    assertThat(peer.readBuffer).isNull();
    assertThat(claimed.position())
        .as("disconnect must not reset a buffer it does not own via the field")
        .isEqualTo(1);
    // cleanup
    claimed.clear();
    ByteBufferPool.returnObject(claimed);
  }
}
