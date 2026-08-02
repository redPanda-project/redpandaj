package im.redpanda.flaschenpost;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import im.redpanda.core.KademliaId;
import im.redpanda.core.ServerContext;
import im.redpanda.crypt.CryptoUtils;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.SecureRandom;
import org.bouncycastle.crypto.params.X25519PublicKeyParameters;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for L2 (bug hunt 2026-07-26) on the packet-parsing paths: a garlic message or
 * flaschenpost layer whose ephemeral X25519 key is a small-order point must be dropped like any
 * other bad packet. Before the check in {@code CryptoUtils.x25519}, BouncyCastle's own
 * contributory-behaviour failure surfaced as an unchecked {@link IllegalStateException} — these
 * paths catch only {@code GeneralSecurityException}, so a remote peer could throw an unchecked
 * exception onto a reader thread with a 32-byte payload.
 */
class GarlicMessageDegenerateKeyTest {

  private static final SecureRandom RANDOM = new SecureRandom();

  @Test
  void garlicMessageWithDegenerateEphemeralKey_isDroppedWithoutThrowing() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();

    GarlicMessage message =
        new GarlicMessage(serverContext, buildPacketWithZeroEphemeralKey(serverContext.getNonce()));

    assertThat(message.isTargetedToUs()).isTrue();
    assertThatCode(message::parseContent).doesNotThrowAnyException();
    assertThat(message.getGMContent()).isEmpty();
  }

  @Test
  void decryptPayload_reportsTheDegenerateKeyAsAnInvalidKey() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();

    GarlicMessage message =
        new GarlicMessage(serverContext, buildPacketWithZeroEphemeralKey(serverContext.getNonce()));

    assertThatThrownBy(message::decryptPayload).isInstanceOf(InvalidKeyException.class);
  }

  @Test
  void flaschenpostLayerWithDegenerateEphemeralKey_isDroppedWithoutThrowing() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();

    // a well-formed MS04 layer body whose ephemeral_pub is an all-zero (degenerate) point
    byte[] ciphertext = new byte[64];
    RANDOM.nextBytes(ciphertext);
    ByteBuffer body = ByteBuffer.allocate(FlaschenpostV2.BODY_HEADER_LEN + ciphertext.length);
    byte[] nonce = new byte[CryptoUtils.GCM_NONCE_LEN];
    RANDOM.nextBytes(nonce);
    body.put(nonce);
    body.put(new byte[CryptoUtils.X25519_KEY_LEN]);
    body.putInt(ciphertext.length);
    body.put(ciphertext);

    byte[] packet =
        FlaschenpostV2.buildPacket(RANDOM.nextInt(), serverContext.getNonce(), body.array());

    assertThatCode(() -> GarlicRouter.handle(serverContext, packet)).doesNotThrowAnyException();
  }

  /**
   * Builds a syntactically valid garlic-message v2 packet addressed to {@code destination} whose
   * ephemeral public key is the all-zero point. The ciphertext is random — the key agreement fails
   * long before the GCM tag is ever checked.
   */
  private static byte[] buildPacketWithZeroEphemeralKey(KademliaId destination) {
    byte[] nonce = new byte[GarlicMessage.NONCE_LEN];
    RANDOM.nextBytes(nonce);
    byte[] ciphertext = new byte[CryptoUtils.GCM_TAG_LEN + 16];
    RANDOM.nextBytes(ciphertext);

    int overallLength =
        1
            + 4
            + KademliaId.ID_LENGTH_BYTES
            + GarlicMessage.NONCE_LEN
            + GarlicMessage.EPHEMERAL_KEY_LEN
            + 4
            + ciphertext.length;

    ByteBuffer packet = ByteBuffer.allocate(overallLength);
    packet.put(GarlicMessage.VERSION);
    packet.putInt(overallLength - 1 - 4);
    packet.put(destination.getBytes());
    packet.put(nonce);
    packet.put(new byte[GarlicMessage.EPHEMERAL_KEY_LEN]); // degenerate ephemeral key
    packet.putInt(ciphertext.length);
    packet.put(ciphertext);
    return packet.array();
  }

  /** Sanity: the all-zero point really is rejected by the primitive this test relies on. */
  @Test
  void allZeroPointIsRejectedByTheAgreement() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    assertThatThrownBy(
            () ->
                CryptoUtils.x25519(
                    serverContext.getNodeId().getEncryptionKey(),
                    new X25519PublicKeyParameters(new byte[CryptoUtils.X25519_KEY_LEN], 0)))
        .isInstanceOf(InvalidKeyException.class);
  }
}
