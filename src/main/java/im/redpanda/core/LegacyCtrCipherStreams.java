package im.redpanda.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import lombok.extern.slf4j.Slf4j;

/**
 * Pre-MS03 TCP stream encryption: AES-CTR stream ciphers without authentication. The OutputStreams
 * are for encryption and the InputStreams for decryption.
 *
 * @deprecated for removal together with protocol-v22 support; v23 connections use {@link
 *     GcmFramedStreams} (framed AES-256-GCM).
 */
@Deprecated(forRemoval = true)
@Slf4j
public class LegacyCtrCipherStreams implements PeerChiperStreams {

  private final PeerOutputStream peerOutputStream;
  private final PeerInputStream peerInputStream;

  private final CipherInputStreamByteBuffer cipherInputStream;
  private final CipherOutputStreamByteBuffer cipherOutputStream;

  public LegacyCtrCipherStreams(
      PeerOutputStream peerOutputStream,
      PeerInputStream peerInputStream,
      CipherInputStreamByteBuffer cipherInputStream,
      CipherOutputStreamByteBuffer cipherOutputStream) {
    this.peerOutputStream = peerOutputStream;
    this.peerInputStream = peerInputStream;
    this.cipherInputStream = cipherInputStream;
    this.cipherOutputStream = cipherOutputStream;
  }

  /**
   * Input has to be in read mode, output has to be in default (write) mode.
   *
   * @param input
   * @param output
   */
  @Override
  public void encrypt(ByteBuffer input, ByteBuffer output) {

    /**
     * encrypt so we have to use the outputstreams The bytes will flow the following way: input ->
     * cipherOutputStream -> peerOutputStream -> output
     */
    try {

      int r = output.remaining();

      if (r < input.remaining()) {
        peerOutputStream.setByteBuffer(output);
        cipherOutputStream.write(input, r);
        cipherOutputStream.flush();
      } else {
        peerOutputStream.setByteBuffer(output);
        cipherOutputStream.write(input);
        cipherOutputStream.flush();
      }
    } catch (IOException e) {
      log.error("legacy CTR stream encryption failed", e);
    }
  }

  /**
   * Input has to be in read mode, output has to be in default (write) mode.
   *
   * @param input
   * @param output
   */
  @Override
  public void decrypt(ByteBuffer input, ByteBuffer output) {

    /**
     * decrypt so we have to use the inputStreams The bytes will flow the following way: input ->
     * peerInputStream -> cipherInputStream -> output
     */
    try {
      peerInputStream.setByteBuffer(input);
      while (input.hasRemaining()) {
        cipherInputStream.read(output);
      }
    } catch (IOException e) {
      log.error("legacy CTR stream decryption failed", e);
    }
  }
}
