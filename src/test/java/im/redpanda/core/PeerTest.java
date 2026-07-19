package im.redpanda.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import im.redpanda.core.exceptions.PeerProtocolException;
import im.redpanda.crypt.Utils;
import java.nio.ByteBuffer;
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

  private void setUpTestCipherStreams(Peer peer) {
    // Same 32-byte key for both directions so the stream can decrypt its own
    // frames in these loopback tests (v23 GCM framed streams).
    byte[] key = new byte[32];
    peer.setPeerChiperStreams(new GcmFramedStreams(key, key));
  }
}
