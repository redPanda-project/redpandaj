package im.redpanda.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import im.redpanda.core.Command;
import im.redpanda.core.ServerContext;
import im.redpanda.identity.NodeId;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * TD186: {@link ByteBufferPool#borrowObject(Integer)} returns {@code null} when the pool cannot
 * hand out a buffer at all — it catches commons-pool's {@code NoSuchElementException} and gives up
 * after one failed replacement of an invalid buffer. Five call sites dereferenced the result
 * immediately ({@link Peer#decryptInputData}, three in {@link ConnectionHandler}, one in {@code
 * ListenConsole}), plus the one in {@link ConnectionReaderThread#readConnection} found alongside
 * them, so an exhausted pool turned into an NPE on the selector or reader thread instead of a clean
 * failure of the operation.
 *
 * <p>The null is produced through the real path rather than through a test hook: the pool is capped
 * at zero objects per key with {@code blockWhenExhausted=false} and its idle buffers are destroyed,
 * so {@code pool.borrowObject} throws "Pool exhausted" for every size class. The pool is a JVM-wide
 * static, hence the save/restore around each test — surefire runs the tests of one fork
 * sequentially, so no other test observes the capped pool.
 */
class ByteBufferPoolBorrowFailureTest {

  static {
    ByteBufferPool.init();
  }

  private int maxTotalPerKeyBefore;
  private boolean blockWhenExhaustedBefore;

  private ServerSocketChannel serverSocket;
  private SocketChannel remoteSide;
  private SocketChannel peerSide;

  @BeforeEach
  void rememberPoolSettings() {
    GenericKeyedObjectPool<Integer, ByteBuffer> pool = ByteBufferPool.getPool();
    maxTotalPerKeyBefore = pool.getMaxTotalPerKey();
    blockWhenExhaustedBefore = pool.getBlockWhenExhausted();
  }

  @AfterEach
  void restorePoolSettings() throws IOException {
    GenericKeyedObjectPool<Integer, ByteBuffer> pool = ByteBufferPool.getPool();
    pool.setMaxTotalPerKey(maxTotalPerKeyBefore);
    pool.setBlockWhenExhausted(blockWhenExhaustedBefore);

    for (SocketChannel channel : new SocketChannel[] {remoteSide, peerSide}) {
      if (channel != null && channel.isOpen()) {
        channel.close();
      }
    }
    if (serverSocket != null && serverSocket.isOpen()) {
      serverSocket.close();
    }
  }

  /**
   * Caps the pool at zero objects per key and destroys the idle ones, so every borrow — of any size
   * class — fails. Call it after the test's own setup has taken whatever buffers it needs.
   */
  private void exhaustPool() {
    GenericKeyedObjectPool<Integer, ByteBuffer> pool = ByteBufferPool.getPool();
    pool.setBlockWhenExhausted(false);
    pool.setMaxTotalPerKey(0);
    // Without this a borrow would be served from the idle deque and never reach the capped
    // creation path.
    pool.clear();
  }

  @Test
  void borrowObject_whenThePoolCannotHandOutABuffer_returnsNull() {
    exhaustPool();

    assertThat(ByteBufferPool.borrowObject(64)).isNull();
    assertThat(ByteBufferPool.borrowObject(1024 * 1024)).isNull();
  }

  /**
   * {@link Peer#decryptInputData} grows the plaintext buffer by borrowing a bigger one. The {@code
   * System.arraycopy} right after the borrow dereferenced it, so an exhausted pool became an NPE
   * out of the read path; it is a {@link PeerProtocolException} now, which the callers already
   * handle by dropping the connection.
   */
  @Test
  void decryptInputData_whenTheReadBufferCannotGrow_failsWithAProtocolException() throws Exception {
    Peer peer = new Peer("127.0.0.1", 0, new NodeId());
    // Same 32-byte key for both directions so the stream decrypts its own frames (see PeerTest).
    byte[] key = new byte[32];
    peer.setPeerChiperStreams(new GcmFramedStreams(key, key));
    // Too small for the 24 bytes of plaintext below, so decryptInputData takes its grow branch.
    ByteBuffer tooSmall = ByteBuffer.allocate(16);
    peer.readBuffer = tooSmall;

    ByteBuffer plaintext = ByteBuffer.allocate(64);
    plaintext.putLong(1L);
    plaintext.putLong(2L);
    plaintext.putLong(3L);
    plaintext.flip();
    ByteBuffer ciphertext = ByteBuffer.allocate(128);
    peer.getPeerChiperStreams().encrypt(plaintext, ciphertext);

    exhaustPool();

    assertThatThrownBy(() -> peer.decryptInputData(ciphertext))
        .isInstanceOf(PeerProtocolException.class)
        .hasMessageContaining("no buffer");
    assertThat(peer.readBuffer)
        .as("the old, still valid buffer must neither be replaced nor returned to the pool")
        .isSameAs(tooSmall);
  }

  /**
   * {@code ConnectionHandler.copyRemainingReadBytesToPeerBuffer} hands the handshake bytes that
   * arrived coalesced after the first PING frame over to the peer's read buffer. Private, so
   * reflection — the same pattern {@code ConnectionHandlerCopyRemainingReadBytesTest} uses for its
   * two success branches.
   */
  @Test
  void copyRemainingReadBytesToPeerBuffer_whenTheBorrowFails_disconnectsInsteadOfNpe()
      throws Exception {
    ConnectionHandler connectionHandler =
        new ConnectionHandler(ServerContext.buildDefaultServerContext(), false);
    Peer peer = new Peer("127.0.0.1", 0, new NodeId());
    peer.setConnected(true);
    assertThat(peer.readBuffer).as("precondition: no buffer claimed yet").isNull();

    ByteBuffer leftovers = ByteBuffer.wrap(new byte[] {10, 20, 30});

    exhaustPool();

    Method method =
        ConnectionHandler.class.getDeclaredMethod(
            "copyRemainingReadBytesToPeerBuffer", ByteBuffer.class, Peer.class);
    method.setAccessible(true);

    assertThatCode(() -> method.invoke(connectionHandler, leftovers, peer))
        .doesNotThrowAnyException();

    assertThat(peer.readBuffer).isNull();
    assertThat(peer.isConnected())
        .as("the leftovers cannot be buffered, so the connection must be torn down")
        .isFalse();
  }

  /**
   * The reader thread borrows the plaintext buffer when the peer has none claimed. {@code
   * decryptInputData} dereferences it one line later, so this had to be guarded too — and the
   * ciphertext already in the per-thread scratch buffer has to be cleared, otherwise it would be
   * prefixed onto the bytes of the next peer this thread services (the TD009 invariant).
   */
  @Test
  void readConnection_whenNoPlaintextBufferCanBeBorrowed_disconnectsInsteadOfNpe()
      throws Exception {
    serverSocket = ServerSocketChannel.open();
    serverSocket.bind(new InetSocketAddress("127.0.0.1", 0));
    remoteSide = SocketChannel.open(serverSocket.getLocalAddress());
    peerSide = serverSocket.accept();

    Peer peer = new Peer("127.0.0.1", 0, new NodeId());
    peer.setSocketChannel(peerSide);
    peer.setConnected(true);
    peer.writeBuffer = ByteBuffer.allocate(1024);
    peer.writeBufferCrypted = ByteBuffer.allocate(1024);
    byte[] key = new byte[32];
    peer.setPeerChiperStreams(new GcmFramedStreams(key, key));
    assertThat(peer.readBuffer).as("precondition: the reader has to borrow one").isNull();

    ByteBuffer in = ByteBuffer.allocate(8);
    in.put(Command.PONG);
    in.flip();
    ByteBuffer out = ByteBuffer.allocate(1024);
    peer.getPeerChiperStreams().encrypt(in, out);
    out.flip();
    while (out.hasRemaining()) {
      remoteSide.write(out);
    }

    ConnectionReaderThread reader =
        new ConnectionReaderThread(new ServerContext(), ConnectionReaderThread.STD_TIMEOUT);

    exhaustPool();

    assertThatCode(() -> reader.readConnection(peer)).doesNotThrowAnyException();

    assertThat(peer.isConnected()).isFalse();
    assertThat(peer.readBuffer).isNull();
    assertThat(reader.myReaderBuffer.position())
        .as("the scratch buffer must be left ready for the next peer (TD009)")
        .isZero();
  }
}
