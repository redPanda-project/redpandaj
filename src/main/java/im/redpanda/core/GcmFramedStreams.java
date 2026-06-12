package im.redpanda.core;

import im.redpanda.core.exceptions.PeerProtocolException;
import im.redpanda.crypt.CryptoUtils;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import javax.crypto.AEADBadTagException;

/**
 * Protocol-v23 TCP stream encryption: framed AES-256-GCM with per-direction keys and counter
 * nonces.
 *
 * <p>Frame format (each direction):
 *
 * <pre>
 * [4 length][12 nonce][ciphertext + 16-byte GCM tag]
 * </pre>
 *
 * <ul>
 *   <li>{@code length} = number of bytes after the length field (nonce + ciphertext + tag).
 *   <li>{@code nonce} = unsigned 96-bit big-endian frame counter, starts at 0, incremented per
 *       frame per direction; the receiver enforces the expected counter value (replay/reorder
 *       protection).
 *   <li>Any authentication failure or framing violation throws a {@link PeerProtocolException} —
 *       the connection must be dropped, a flipped bit never yields silently corrupted plaintext.
 * </ul>
 */
public class GcmFramedStreams implements PeerChiperStreams {

  /** Maximum plaintext bytes per frame; larger writes are split into multiple frames. */
  public static final int MAX_PLAINTEXT_PER_FRAME = 32 * 1024;

  /** Frame overhead: 4-byte length field + 12-byte nonce + 16-byte GCM tag. */
  public static final int FRAME_OVERHEAD = 4 + CryptoUtils.GCM_NONCE_LEN + CryptoUtils.GCM_TAG_LEN;

  private static final int MAX_FRAME_PAYLOAD_LEN =
      CryptoUtils.GCM_NONCE_LEN + MAX_PLAINTEXT_PER_FRAME + CryptoUtils.GCM_TAG_LEN;

  private final byte[] sendKey;
  private final byte[] receiveKey;

  private long sendCounter = 0;
  private long receiveCounter = 0;

  /** Buffered inbound ciphertext of a not-yet-complete frame (write mode). */
  private ByteBuffer inbound = ByteBuffer.allocate(4096);

  public GcmFramedStreams(byte[] sendKey, byte[] receiveKey) {
    if (sendKey.length != CryptoUtils.AES_KEY_LEN || receiveKey.length != CryptoUtils.AES_KEY_LEN) {
      throw new IllegalArgumentException("GCM stream keys must be 32 bytes");
    }
    this.sendKey = sendKey.clone();
    this.receiveKey = receiveKey.clone();
  }

  @Override
  public void encrypt(ByteBuffer input, ByteBuffer output) {
    while (input.hasRemaining()) {
      int chunk =
          Math.min(
              Math.min(input.remaining(), MAX_PLAINTEXT_PER_FRAME),
              output.remaining() - FRAME_OVERHEAD);
      if (chunk <= 0) {
        // output buffer full — remaining plaintext stays in the input buffer for the next round
        return;
      }

      byte[] plaintext = new byte[chunk];
      input.get(plaintext);

      byte[] nonce = nonceFromCounter(sendCounter++);
      byte[] ciphertextAndTag;
      try {
        ciphertextAndTag = CryptoUtils.encryptGcm(sendKey, nonce, plaintext, null);
      } catch (GeneralSecurityException e) {
        // only possible through programming errors (invalid key/params), never data-dependent
        throw new IllegalStateException("GCM frame encryption failed", e);
      }

      output.putInt(CryptoUtils.GCM_NONCE_LEN + ciphertextAndTag.length);
      output.put(nonce);
      output.put(ciphertextAndTag);
    }
  }

  @Override
  public void decrypt(ByteBuffer input, ByteBuffer output) throws PeerProtocolException {
    appendToInbound(input);

    while (true) {
      if (inbound.position() < 4) {
        return;
      }
      int payloadLen = inbound.getInt(0);
      if (payloadLen < CryptoUtils.GCM_NONCE_LEN + CryptoUtils.GCM_TAG_LEN
          || payloadLen > MAX_FRAME_PAYLOAD_LEN) {
        throw new PeerProtocolException("invalid GCM frame length: %s".formatted(payloadLen));
      }
      if (inbound.position() < 4 + payloadLen) {
        return;
      }

      int plaintextLen = payloadLen - CryptoUtils.GCM_NONCE_LEN - CryptoUtils.GCM_TAG_LEN;
      if (output.remaining() < plaintextLen) {
        // not enough space in the output buffer — keep the complete frame buffered; the caller
        // sizes its buffer via pendingDecryptBytes() and calls again
        return;
      }

      // extract one complete frame
      inbound.flip();
      inbound.getInt();
      byte[] nonce = new byte[CryptoUtils.GCM_NONCE_LEN];
      inbound.get(nonce);
      byte[] ciphertextAndTag = new byte[payloadLen - CryptoUtils.GCM_NONCE_LEN];
      inbound.get(ciphertextAndTag);
      inbound.compact();

      byte[] expectedNonce = nonceFromCounter(receiveCounter);
      if (!java.util.Arrays.equals(nonce, expectedNonce)) {
        throw new PeerProtocolException(
            "unexpected GCM frame nonce (expected counter %s)".formatted(receiveCounter));
      }

      byte[] plaintext;
      try {
        plaintext = CryptoUtils.decryptGcm(receiveKey, nonce, ciphertextAndTag, null);
      } catch (AEADBadTagException e) {
        throw new PeerProtocolException("GCM frame authentication failed");
      } catch (GeneralSecurityException e) {
        throw new PeerProtocolException("GCM frame decryption failed: %s".formatted(e));
      }
      receiveCounter++;

      output.put(plaintext);
    }
  }

  @Override
  public int pendingDecryptBytes() {
    return inbound.position();
  }

  private void appendToInbound(ByteBuffer input) {
    if (inbound.remaining() < input.remaining()) {
      int newSize = Math.max(inbound.capacity() * 2, inbound.position() + input.remaining());
      ByteBuffer bigger = ByteBuffer.allocate(newSize);
      inbound.flip();
      bigger.put(inbound);
      inbound = bigger;
    }
    inbound.put(input);
  }

  /** Builds the 12-byte big-endian nonce for the given frame counter. */
  static byte[] nonceFromCounter(long counter) {
    byte[] nonce = new byte[CryptoUtils.GCM_NONCE_LEN];
    ByteBuffer.wrap(nonce, 4, 8).putLong(counter);
    return nonce;
  }
}
