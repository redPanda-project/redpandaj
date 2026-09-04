package im.redpanda.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import im.redpanda.core.Server;
import im.redpanda.core.ServerContext;
import im.redpanda.identity.KademliaId;
import im.redpanda.identity.NodeId;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.SecureRandom;
import org.bouncycastle.crypto.params.X25519PrivateKeyParameters;
import org.junit.jupiter.api.Test;

/**
 * T120d — {@code RuntimeException: calculateSharedSecret: missing the peers public NodeId keys},
 * seen once on each Hetzner node two seconds after the simultaneous restart of deploy #9.
 *
 * <p>Shape on the testnet: after a restart a node knows the other one twice — a {@link Peer}
 * restored for its identity, which carries the public keys but whose connection details were
 * cleared at some point, and an id-less placeholder that carries the address (reseed or gossip).
 * The outbound thread dials through the placeholder, so {@code OutboundHandler.connectTo} has no
 * NodeId to hand to the handshake. When the identity arrives, {@code PeerList.updateKademliaId}
 * (T120a) hands the handshake over to the registered peer object — which already has the keys — and
 * {@code parseHandshake}'s "the peer already has a keyed NodeId" branch goes straight to status -1
 * without copying them onto the handshake. {@code hasPublicKey()} then answered from the peer
 * ("yes") while the key schedule read the handshake's own field (empty) and threw.
 */
class HandshakeKeyPreservationTest {

  static {
    java.security.Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  /**
   * The reproduction: dial through an id-less placeholder for a node whose keyed {@link Peer} is
   * already registered, then complete the key exchange the way the selector thread does.
   */
  @Test
  void dialThroughAPlaceholderStillFindsThePeersPublicKey() throws Exception {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    PeerList peerList = serverContext.getPeerList();

    // restored for its identity, keys included, connection details cleared (5 of the 8 peers
    // restored on the testnet node had no address)
    NodeId identity = NodeId.generateWithSimpleKey();
    Peer restored = new Peer("10.3.0.2", 59558, identity);
    peerList.add(restored);
    peerList.clearConnectionDetails(restored);

    // the address we actually dial, known without an identity
    Peer placeholder = new Peer("10.3.0.2", 59558);
    peerList.add(placeholder);

    try (SocketChannel channel = SocketChannel.open()) {
      // OutboundHandler.connectTo: no NodeId on the placeholder, so none is handed over
      PeerInHandshake handshake = new PeerInHandshake("10.3.0.2", placeholder, channel);
      assertThat(placeholder.getNodeId()).as("precondition: we dial an id-less peer").isNull();

      boolean parsed =
          ConnectionReaderThread.parseHandshake(
              serverContext, handshake, handshakeBytes(identity.getKademliaId()));

      assertThat(parsed).isTrue();
      assertThat(handshake.getPeer())
          .as("the handshake continues on the registered peer for that identity (T120a)")
          .isSameAs(restored);
      assertThat(handshake.getStatus())
          .as("the peer already has the keys, so no REQUEST_PUBLIC_KEY round trip is needed")
          .isEqualTo(-1);
      assertThat(handshake.hasPublicKey()).isTrue();

      // ... and the key schedule must find the very keys hasPublicKey() just promised
      exchangeEphemeralKeys(handshake);
      assertThatCode(() -> handshake.calculateSharedSecret(serverContext))
          .doesNotThrowAnyException();
    }
  }

  /**
   * The invariant behind it, stated directly: whatever {@link PeerInHandshake#hasPublicKey()}
   * answers, the key schedule must be able to act on it.
   */
  @Test
  void hasPublicKeyAndTheKeyScheduleReadTheSameNodeId() throws Exception {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    NodeId identity = NodeId.generateWithSimpleKey();

    try (SocketChannel channel = SocketChannel.open()) {
      Peer peer = new Peer("10.3.0.3", 59558, identity);
      PeerInHandshake handshake = new PeerInHandshake("10.3.0.3", peer, channel);
      handshake.setIdentity(identity.getKademliaId());
      // the handshake itself was never told the keys — only the peer has them
      assertThat(handshake.hasPublicKey()).isTrue();

      exchangeEphemeralKeys(handshake);
      assertThatCode(() -> handshake.calculateSharedSecret(serverContext))
          .doesNotThrowAnyException();
    }
  }

  /**
   * Without keys anywhere the handshake must still say "no" and take the REQUEST_PUBLIC_KEY path.
   */
  @Test
  void aKeylessPeerStillReportsNoPublicKey() throws Exception {
    KademliaId identity = new KademliaId();

    try (SocketChannel channel = SocketChannel.open()) {
      Peer peer = new Peer("10.3.0.4", 59558, new NodeId(identity));
      PeerInHandshake handshake = new PeerInHandshake("10.3.0.4", peer, channel);
      handshake.setIdentity(identity);

      assertThat(handshake.hasPublicKey()).isFalse();
      assertThat(handshake.getNodeId().hasKey()).isFalse();
    }
  }

  /**
   * Keys are only taken from the peer object while they belong to the identity this connection
   * announced — otherwise they would derive a session secret for a different node.
   */
  @Test
  void keysOfADifferentIdentityAreNotUsed() throws Exception {
    NodeId someoneElse = NodeId.generateWithSimpleKey();

    try (SocketChannel channel = SocketChannel.open()) {
      Peer peer = new Peer("10.3.0.5", 59558, someoneElse);
      PeerInHandshake handshake = new PeerInHandshake("10.3.0.5", peer, channel);
      handshake.setIdentity(new KademliaId());

      assertThat(handshake.hasPublicKey())
          .as("the peer object's keys are for another identity")
          .isFalse();
    }
  }

  /** A 30-byte v23 handshake announcing {@code identity} and port 59558. */
  private static ByteBuffer handshakeBytes(KademliaId identity) {
    ByteBuffer buffer = ByteBuffer.allocate(30);
    buffer.put(Server.MAGIC.getBytes());
    buffer.put((byte) Server.VERSION);
    buffer.put((byte) 0); // full node
    buffer.put(identity.getBytes());
    buffer.putInt(59558);
    buffer.flip();
    return buffer;
  }

  private static void exchangeEphemeralKeys(PeerInHandshake handshake) {
    handshake.getEphemeralPublicFromUs();
    handshake.setEphemeralPublicFromThem(
        new X25519PrivateKeyParameters(new SecureRandom()).generatePublicKey().getEncoded());
  }
}
