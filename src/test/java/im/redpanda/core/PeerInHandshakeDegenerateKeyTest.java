package im.redpanda.core;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import im.redpanda.core.exceptions.PeerProtocolException;
import im.redpanda.crypt.CryptoUtils;
import java.security.Security;
import org.junit.jupiter.api.Test;

/**
 * Regression test for L2 (bug hunt 2026-07-26) on the handshake path: an ephemeral X25519 key that
 * is a small-order point makes the agreement all-zero, so it must not reach the HKDF that derives
 * the session keys. {@code PeerProtocolException} is the existing "hostile-network noise" channel —
 * {@code ConnectionHandler.handlePeerInHandshake} drops such a handshake quietly and closes the
 * channel instead of reporting a Sentry error per occurrence.
 */
class PeerInHandshakeDegenerateKeyTest {

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  @Test
  void calculateSharedSecret_rejectsADegenerateEphemeralKey() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    PeerInHandshake handshake =
        newHandshakeWith(serverContext, new byte[CryptoUtils.X25519_KEY_LEN]);

    assertThatThrownBy(() -> handshake.calculateSharedSecret(serverContext))
        .isInstanceOf(PeerProtocolException.class)
        .hasMessageContaining("degenerate");
  }

  /** Interop guard: an ephemeral key from a conforming client still completes the key schedule. */
  @Test
  void calculateSharedSecret_acceptsAnHonestEphemeralKey() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    PeerInHandshake theirSide = new PeerInHandshake("127.0.0.1", new Peer("127.0.0.1", 1234), null);
    PeerInHandshake handshake =
        newHandshakeWith(serverContext, theirSide.getEphemeralPublicFromUs());

    assertThatCode(() -> handshake.calculateSharedSecret(serverContext)).doesNotThrowAnyException();
  }

  private static PeerInHandshake newHandshakeWith(
      ServerContext serverContext, byte[] ephemeralFromThem) {
    Peer peer = new Peer("127.0.0.1", 1234);
    PeerInHandshake handshake = new PeerInHandshake("127.0.0.1", peer, null);
    handshake.setNodeId(NodeId.importPublic(new NodeId().exportPublic()));
    handshake.getEphemeralPublicFromUs(); // generate our own ephemeral keypair
    handshake.setEphemeralPublicFromThem(ephemeralFromThem);
    return handshake;
  }
}
