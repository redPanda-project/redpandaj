package im.redpanda.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import im.redpanda.core.exceptions.PeerProtocolException;
import im.redpanda.crypt.Utils;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Random;
import org.junit.Test;

public class PeerTest {

  static {
    ByteBufferPool.init();
  }

  @Test
  public void getNodeId() {}

  @Test
  public void equalsIpAndPort() {

    Peer peer = new Peer("1.1.1.1", 123);
    Peer peer2 = new Peer("1.1.1.1", 123);
    Peer peer3 = new Peer("1.1.1.2", 123);
    Peer peer4 = new Peer("1.1.1.1", 124);

    assertTrue(peer.equalsIpAndPort(peer2));
    assertFalse(peer.equalsIpAndPort(peer3));
    assertFalse(peer.equalsIpAndPort(peer4));
  }

  @Test
  public void equalsNonce() {

    Peer peer = new Peer("1.1.1.1", 123);
    Peer peer2 = new Peer("1.1.1.1", 123);
    Peer peer3 = new Peer("1.1.1.1", 123);

    KademliaId id1 = new KademliaId();
    KademliaId id2 = new KademliaId();

    assertNotNull(peer);
    assertNotNull(id1);

    peer.setNodeId(new NodeId(id1));
    peer2.setNodeId(new NodeId(id1));
    peer3.setNodeId(new NodeId(id2));

    assertTrue(peer.equalsNonce(peer2));
    assertFalse(peer.equalsNonce(peer3));
  }

  @Test
  public void equalsInstance() {

    Peer peer = new Peer("1.1.1.1", 123);
    Peer peer2 = new Peer("1.1.1.1", 123);

    assertTrue(peer.equalsInstance(peer));
    assertFalse(peer.equalsInstance(peer2));
  }

  @Test
  public void setNodeId() {
    Peer peer = new Peer("1.1.1.1", 123);
    KademliaId id1 = new KademliaId();
    peer.setNodeId(new NodeId(id1));
    assertTrue(peer.getKademliaId().equals(id1));
  }

  @Test
  public void peerIsHigher() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();

    Peer peer = new Peer("1.1.1.1", 123);
    Peer peer2 = new Peer("1.1.1.1", 123);

    KademliaId kademliaId =
        KademliaId.fromFirstBytes(
            Utils.parseAsHexOrBase58(
                "000000000000000000000000000000000000000000000000000000000000000000000000"));
    KademliaId kademliaId2 =
        KademliaId.fromFirstBytes(
            Utils.parseAsHexOrBase58(
                "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"));

    peer.setNodeId(new NodeId(kademliaId));
    peer2.setNodeId(new NodeId(kademliaId2));

    assertFalse(peer.peerIsHigher(serverContext));
    assertTrue(peer2.peerIsHigher(serverContext));
  }

  @Test
  public void decryptInputDataNoBytes() throws PeerProtocolException {
    Peer peer = new Peer("ip", 59558, new NodeId());

    setUpTestCipherStreams(peer);

    peer.readBuffer = ByteBuffer.allocate(80);

    ByteBuffer bufferIn = ByteBuffer.allocate(60);
    ByteBuffer bufferOut = ByteBuffer.allocate(60);
    bufferIn.flip();
    peer.getPeerChiperStreams().encrypt(bufferIn, bufferOut);

    int decryptedBytes = peer.decryptInputData(bufferOut);
    assertThat(decryptedBytes).isZero();
  }

  @Test
  public void decryptInputDataSimpleBytes() throws PeerProtocolException {
    Peer peer = new Peer("ip", 59558, new NodeId());

    setUpTestCipherStreams(peer);

    peer.readBuffer = ByteBuffer.allocate(80);

    long longToTest = new Random().nextLong();

    ByteBuffer bufferIn = ByteBuffer.allocate(60);
    bufferIn.putLong(longToTest);
    ByteBuffer bufferOut = ByteBuffer.allocate(60);
    bufferIn.flip();
    peer.getPeerChiperStreams().encrypt(bufferIn, bufferOut);
    // decryptInputData reports CONSUMED ciphertext bytes — with the framed
    // GCM streams that is plaintext + frame overhead, not the plaintext size.
    int cipherBytes = bufferOut.position();

    int decryptedBytes = peer.decryptInputData(bufferOut);
    peer.readBuffer.flip();
    assertThat(decryptedBytes).isEqualTo(cipherBytes);
    assertThat(peer.readBuffer.remaining()).isEqualTo(8);
    assertThat(peer.readBuffer.getLong()).isEqualTo(longToTest);
  }

  @Test
  public void decryptInputDataTooSmallReadBuffer() throws PeerProtocolException {
    Peer peer = new Peer("ip", 59558, new NodeId());

    setUpTestCipherStreams(peer);

    peer.readBuffer = ByteBufferPool.borrowObject(16);
    assertThat(peer.readBuffer.remaining()).isEqualTo(16);

    long longToTest = new Random().nextLong();

    ByteBuffer bufferIn = ByteBuffer.allocate(60);
    bufferIn.putLong(longToTest);
    bufferIn.putLong(longToTest);
    bufferIn.putLong(longToTest);
    ByteBuffer bufferOut = ByteBuffer.allocate(60);
    bufferIn.flip();
    peer.getPeerChiperStreams().encrypt(bufferIn, bufferOut);
    int cipherBytes = bufferOut.position();

    int decryptedBytes = peer.decryptInputData(bufferOut);
    peer.readBuffer.flip();
    assertThat(decryptedBytes).isEqualTo(cipherBytes);
    assertThat(peer.readBuffer.remaining()).isEqualTo(24);
    assertThat(peer.readBuffer.getLong()).isEqualTo(longToTest);
    assertThat(peer.readBuffer.getLong()).isEqualTo(longToTest);
    assertThat(peer.readBuffer.getLong()).isEqualTo(longToTest);
  }

  /**
   * Regression for REDPANDAJ-2DT / REDPANDAJ-2DV: when {@code decryptInputData} must grow the
   * plaintext buffer it returns the old, too-small buffer instance to the {@link ByteBufferPool}
   * and stores a new, larger one in {@code peer.readBuffer}. A caller (ConnectionReaderThread) that
   * captured a reference to {@code peer.readBuffer} <em>before</em> the call must therefore re-read
   * the field afterwards — continuing to use the stale reference flips/compacts a buffer that is
   * already idle in the pool, corrupting it to {@code pos=0 lim=0} and surfacing later as
   * "borrowObject found an invalid ByteBuffer". This test pins the swap-and-return contract that
   * the fix relies on.
   */
  @Test
  public void decryptInputDataGrow_swapsReadBufferAndReturnsOldInstanceToPool()
      throws PeerProtocolException {
    Peer peer = new Peer("ip", 59558, new NodeId());

    setUpTestCipherStreams(peer);

    peer.readBuffer = ByteBufferPool.borrowObject(16);
    // Mimic ConnectionReaderThread.readConnection capturing the reference before decrypting.
    ByteBuffer staleReferenceBeforeDecrypt = peer.readBuffer;
    assertThat(staleReferenceBeforeDecrypt.remaining()).isEqualTo(16);

    long longToTest = new Random().nextLong();
    ByteBuffer bufferIn = ByteBuffer.allocate(60);
    bufferIn.putLong(longToTest);
    bufferIn.putLong(longToTest);
    bufferIn.putLong(longToTest);
    ByteBuffer bufferOut = ByteBuffer.allocate(60);
    bufferIn.flip();
    peer.getPeerChiperStreams().encrypt(bufferIn, bufferOut);

    peer.decryptInputData(bufferOut);

    // The buffer had to grow, so peer.readBuffer must now be a different, larger instance...
    assertThat(peer.readBuffer)
        .as("decryptInputData must swap in a larger buffer on growth")
        .isNotSameAs(staleReferenceBeforeDecrypt);
    assertThat(peer.readBuffer.capacity()).isGreaterThan(staleReferenceBeforeDecrypt.capacity());

    // ...and the old instance must have been handed back to the pool in a valid, idle state
    // (pos=0, lim=cap). The bug corrupted exactly this buffer via a stale caller reference.
    assertThat(staleReferenceBeforeDecrypt.position()).isEqualTo(0);
    assertThat(staleReferenceBeforeDecrypt.limit())
        .isEqualTo(staleReferenceBeforeDecrypt.capacity());
  }

  /**
   * Regression for TD008: {@code disconnect()} nulls {@code writeBuffer} under {@code
   * writeBufferLock}, while {@code sendPing()} used to check/use it before acquiring that lock — a
   * full disconnect() (lock, null the field, unlock) could complete in the window between the
   * pre-lock check and {@code tryLock()} succeeding, so the field was null again by the time the
   * locked section dereferenced it. This test pins the end state of that race directly: {@code
   * writeBuffer} is already null when {@code sendPing()} is entered (selectionKey still valid,
   * exactly as a caller mid-{@code tryLock()} would observe it). Must not throw NPE.
   */
  @Test
  public void sendPingWithNullWriteBufferDoesNotThrowNpe() throws Exception {
    try (ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        Selector selector = Selector.open()) {
      serverSocketChannel.configureBlocking(false);
      serverSocketChannel.bind(new InetSocketAddress("127.0.0.1", 0));
      int port = serverSocketChannel.socket().getLocalPort();

      try (SocketChannel client = SocketChannel.open(new InetSocketAddress("127.0.0.1", port))) {
        SocketChannel accepted;
        do {
          accepted = serverSocketChannel.accept();
        } while (accepted == null);
        accepted.configureBlocking(false);
        SelectionKey key = accepted.register(selector, SelectionKey.OP_READ);

        Peer peer = new Peer("127.0.0.1", port, new NodeId());
        peer.setConnected(true);
        peer.setSocketChannel(accepted);
        peer.setSelectionKey(key);
        peer.writeBuffer = null;

        peer.sendPing();

        assertFalse(
            "sendPing must mark the peer disconnected on a null writeBuffer", peer.isConnected());
      }
    }
  }

  /**
   * Companion to {@link #sendPingWithNullWriteBufferDoesNotThrowNpe()}: the normal, non-null
   * writeBuffer path must still queue a PING byte (guards against an overzealous TD008 fix).
   */
  @Test
  public void sendPingWithNonNullWriteBufferPutsPingByte() throws Exception {
    try (ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        Selector selector = Selector.open()) {
      serverSocketChannel.configureBlocking(false);
      serverSocketChannel.bind(new InetSocketAddress("127.0.0.1", 0));
      int port = serverSocketChannel.socket().getLocalPort();

      try (SocketChannel client = SocketChannel.open(new InetSocketAddress("127.0.0.1", port))) {
        SocketChannel accepted;
        do {
          accepted = serverSocketChannel.accept();
        } while (accepted == null);
        accepted.configureBlocking(false);
        SelectionKey key = accepted.register(selector, SelectionKey.OP_READ);

        Peer peer = new Peer("127.0.0.1", port, new NodeId());
        peer.setConnected(true);
        peer.setSocketChannel(accepted);
        peer.setSelectionKey(key);
        peer.writeBuffer = ByteBuffer.allocate(1024);

        peer.sendPing();

        assertEquals(1, peer.writeBuffer.position());
        assertEquals(Command.PING, peer.writeBuffer.get(0));
      }
    }
  }

  /**
   * Regression for TD010: on the (extremely rare) buffer-allocation failure branch of {@code
   * setupConnectionForPeer}, the method used to keep running after calling {@code disconnect()},
   * re-populating socketChannel/selectionKey/cipherStreams on the peer it had just torn down (and,
   * for a non-light-client peer, going on to NPE on the still-null {@code writeBuffer} a few lines
   * later). A genuine allocation failure is not deterministically/safely injectable without either
   * exhausting the shared test-fork heap (risking every other test in the fork) or adding a
   * production-only test seam, so this suite does not attempt to reproduce the OOM branch itself;
   * see the inline comment at the {@code return} in {@code setupConnectionForPeer} for the
   * reasoning. This test instead pins the unaffected happy path (allocation succeeds) so a future
   * refactor of the added early return cannot silently break normal connection setup.
   */
  @Test
  public void setupConnectionForPeer_happyPath_populatesConnectionStateAndBuffers()
      throws Exception {
    try (ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        Selector selector = Selector.open()) {
      serverSocketChannel.configureBlocking(false);
      serverSocketChannel.bind(new InetSocketAddress("127.0.0.1", 0));
      int port = serverSocketChannel.socket().getLocalPort();

      try (SocketChannel client = SocketChannel.open(new InetSocketAddress("127.0.0.1", port))) {
        SocketChannel accepted;
        do {
          accepted = serverSocketChannel.accept();
        } while (accepted == null);
        accepted.configureBlocking(false);
        SelectionKey key = accepted.register(selector, SelectionKey.OP_READ);

        PeerInHandshake peerInHandshake = new PeerInHandshake("127.0.0.1", accepted);
        peerInHandshake.setKey(key);
        peerInHandshake.setLightClient(false);
        peerInHandshake.setProtocolVersion(23);

        Peer peer = new Peer("127.0.0.1", port, new NodeId());

        peer.setupConnectionForPeer(peerInHandshake);

        assertTrue(peer.isConnected());
        assertTrue(peer.isAuthed());
        assertNotNull(peer.writeBuffer);
        assertEquals(300 * 1024, peer.writeBuffer.capacity());
        assertNotNull(peer.writeBufferCrypted);
        assertEquals(300 * 1024, peer.writeBufferCrypted.capacity());
        assertSame(accepted, peer.getSocketChannel());
        assertSame(key, peer.getSelectionKey());
        // non-lightClient path queues UPDATE_REQUEST_TIMESTAMP + ANDROID_UPDATE_REQUEST_TIMESTAMP
        assertEquals(2, peer.writeBuffer.position());
      }
    }
  }

  private void setUpTestCipherStreams(Peer peer) {
    // Same 32-byte key for both directions so the stream can decrypt its own
    // frames in these loopback tests (v23 GCM framed streams).
    byte[] key = new byte[32];
    peer.setPeerChiperStreams(new GcmFramedStreams(key, key));
  }
}
