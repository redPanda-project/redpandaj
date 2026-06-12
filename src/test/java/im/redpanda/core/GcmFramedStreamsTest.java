package im.redpanda.core;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import im.redpanda.core.exceptions.PeerProtocolException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import org.junit.Test;

/** MS03: framed AES-256-GCM TCP stream encryption (protocol v23). */
public class GcmFramedStreamsTest {

  private static final SecureRandom RANDOM = new SecureRandom();

  private static byte[] randomKey() {
    byte[] key = new byte[32];
    RANDOM.nextBytes(key);
    return key;
  }

  /** A connected pair: what A sends, B receives (and vice versa). */
  private static GcmFramedStreams[] pair() {
    byte[] keyAtoB = randomKey();
    byte[] keyBtoA = randomKey();
    return new GcmFramedStreams[] {
      new GcmFramedStreams(keyAtoB, keyBtoA), new GcmFramedStreams(keyBtoA, keyAtoB)
    };
  }

  @Test
  public void roundtripSingleFrame() throws Exception {
    GcmFramedStreams[] streams = pair();

    byte[] message = "hello redpanda v23".getBytes();
    ByteBuffer wire = ByteBuffer.allocate(1024);
    streams[0].encrypt(ByteBuffer.wrap(message), wire);

    assertEquals(message.length + GcmFramedStreams.FRAME_OVERHEAD, wire.position());

    wire.flip();
    ByteBuffer plain = ByteBuffer.allocate(1024);
    streams[1].decrypt(wire, plain);

    plain.flip();
    byte[] decrypted = new byte[plain.remaining()];
    plain.get(decrypted);
    assertArrayEquals(message, decrypted);
  }

  @Test
  public void roundtripWithPartialDeliveryAndMultipleFrames() throws Exception {
    GcmFramedStreams[] streams = pair();

    byte[] message = new byte[100_000]; // forces multiple frames (> MAX_PLAINTEXT_PER_FRAME)
    RANDOM.nextBytes(message);

    ByteBuffer wire = ByteBuffer.allocate(message.length + 10 * GcmFramedStreams.FRAME_OVERHEAD);
    streams[0].encrypt(ByteBuffer.wrap(message), wire);
    wire.flip();

    // deliver the ciphertext in odd-sized chunks to exercise the frame reassembly
    ByteBuffer plain = ByteBuffer.allocate(message.length + 1024);
    int chunkSize = 7777;
    while (wire.hasRemaining()) {
      int n = Math.min(chunkSize, wire.remaining());
      byte[] chunk = new byte[n];
      wire.get(chunk);
      streams[1].decrypt(ByteBuffer.wrap(chunk), plain);
    }

    plain.flip();
    byte[] decrypted = new byte[plain.remaining()];
    plain.get(decrypted);
    assertArrayEquals(message, decrypted);
    assertEquals(0, streams[1].pendingDecryptBytes());
  }

  @Test
  public void flippedBitCausesDecryptionFailureNotSilentCorruption() throws Exception {
    GcmFramedStreams[] streams = pair();

    ByteBuffer wire = ByteBuffer.allocate(1024);
    streams[0].encrypt(ByteBuffer.wrap("attack at dawn".getBytes()), wire);

    // flip one bit in the ciphertext part of the frame
    int ciphertextStart = 4 + 12;
    wire.put(ciphertextStart, (byte) (wire.get(ciphertextStart) ^ 0x01));
    wire.flip();

    ByteBuffer plain = ByteBuffer.allocate(1024);
    PeerProtocolException e =
        assertThrows(PeerProtocolException.class, () -> streams[1].decrypt(wire, plain));
    assertTrue(e.getMessage().contains("authentication failed"));
    assertEquals("no plaintext may be produced", 0, plain.position());
  }

  @Test
  public void replayedFrameIsRejectedByNonceCounter() throws Exception {
    GcmFramedStreams[] streams = pair();

    ByteBuffer wire = ByteBuffer.allocate(1024);
    streams[0].encrypt(ByteBuffer.wrap("frame0".getBytes()), wire);
    wire.flip();
    byte[] frame = new byte[wire.remaining()];
    wire.get(frame);

    ByteBuffer plain = ByteBuffer.allocate(1024);
    streams[1].decrypt(ByteBuffer.wrap(frame), plain);

    // replaying the identical frame must fail (receive counter has advanced)
    assertThrows(
        PeerProtocolException.class,
        () -> streams[1].decrypt(ByteBuffer.wrap(frame), ByteBuffer.allocate(1024)));
  }

  @Test
  public void decryptKeepsFrameBufferedWhenOutputBufferIsTooSmall() throws Exception {
    GcmFramedStreams[] streams = pair();

    byte[] message = new byte[256];
    RANDOM.nextBytes(message);
    ByteBuffer wire = ByteBuffer.allocate(1024);
    streams[0].encrypt(ByteBuffer.wrap(message), wire);
    wire.flip();

    // undersized output: the complete frame must stay buffered, nothing is written
    ByteBuffer tooSmall = ByteBuffer.allocate(message.length - 1);
    streams[1].decrypt(wire, tooSmall);
    assertEquals(0, tooSmall.position());
    assertTrue(streams[1].pendingDecryptBytes() > 0);

    // retry with enough capacity succeeds without re-sending any bytes
    ByteBuffer bigEnough = ByteBuffer.allocate(message.length);
    streams[1].decrypt(ByteBuffer.allocate(0), bigEnough);
    bigEnough.flip();
    byte[] decrypted = new byte[bigEnough.remaining()];
    bigEnough.get(decrypted);
    assertArrayEquals(message, decrypted);
    assertEquals(0, streams[1].pendingDecryptBytes());
  }

  @Test
  public void invalidFrameLengthIsRejected() {
    GcmFramedStreams streams = new GcmFramedStreams(randomKey(), randomKey());

    ByteBuffer bogus = ByteBuffer.allocate(8);
    bogus.putInt(Integer.MAX_VALUE);
    bogus.putInt(0);
    bogus.flip();

    assertThrows(
        PeerProtocolException.class, () -> streams.decrypt(bogus, ByteBuffer.allocate(64)));
  }

  @Test
  public void encryptStopsWhenOutputBufferIsFull() {
    GcmFramedStreams streams = new GcmFramedStreams(randomKey(), randomKey());

    byte[] message = new byte[1000];
    ByteBuffer input = ByteBuffer.wrap(message);
    ByteBuffer smallOutput = ByteBuffer.allocate(100);

    streams.encrypt(input, smallOutput);

    // some plaintext must remain for the next round, output respects its capacity
    assertTrue(input.hasRemaining());
    assertTrue(smallOutput.position() <= smallOutput.capacity());
  }

  @Test
  public void nonceIsBigEndianCounter() {
    assertArrayEquals(
        new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, GcmFramedStreams.nonceFromCounter(0));
    assertArrayEquals(
        new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2}, GcmFramedStreams.nonceFromCounter(258));
  }
}
