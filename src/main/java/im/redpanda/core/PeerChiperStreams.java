package im.redpanda.core;

import im.redpanda.core.exceptions.PeerProtocolException;
import java.nio.ByteBuffer;

/**
 * Per-connection encryption of the TCP byte stream. Implementation: {@link GcmFramedStreams}
 * (protocol v23, framed AES-256-GCM). The v22 AES-CTR implementation was removed together with
 * protocol-v22 support (sdd02 phase 2).
 */
public interface PeerChiperStreams {

  /**
   * Encrypts bytes from {@code input} into {@code output}. Input has to be in read mode, output has
   * to be in default (write) mode. May leave bytes in {@code input} if {@code output} has not
   * enough space.
   */
  void encrypt(ByteBuffer input, ByteBuffer output);

  /**
   * Decrypts bytes from {@code input} into {@code output}. Input has to be in read mode, output has
   * to be in default (write) mode.
   *
   * @throws PeerProtocolException if the stream is corrupted (e.g. failed GCM authentication) — the
   *     connection must be closed
   */
  void decrypt(ByteBuffer input, ByteBuffer output) throws PeerProtocolException;

  /**
   * Number of buffered ciphertext bytes of an incomplete inbound frame (relevant for sizing the
   * plaintext output buffer). Streams without internal framing return 0.
   */
  default int pendingDecryptBytes() {
    return 0;
  }
}
