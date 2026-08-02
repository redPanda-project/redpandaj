package im.redpanda.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

/**
 * REDPANDAJ-2EH regression: the 64 bytes following a plaintext SEND_PUBLIC_KEY are unauthenticated
 * remote input, and BouncyCastle rejects a malformed Ed25519 encoding (non-canonical or small-order
 * point) with {@code IllegalArgumentException: invalid public key}. That exception used to escape
 * {@code parsePlaintextHandshakeCommands} into the generic handler in {@code
 * handlePeerInHandshake}: an error-level Sentry event per hostile packet, the key cancelled but the
 * socket left half-open until {@code PeerJobs.reapStaleHandshakes} closed it. A malformed key must
 * instead be a quiet per-connection rejection, exactly like the KademliaId-mismatch case right
 * below it.
 */
class ConnectionHandlerMalformedPublicKeyTest {

  static {
    java.security.Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  @Test
  void malformedPublicKeyIsRejectedQuietlyAndClosesTheConnection() throws Exception {
    // Verified empirically against BouncyCastle 1.84: both encodings below make
    // Ed25519PublicKeyParameters throw "invalid public key" — all-zero decodes to a small-order
    // point (rejected), all-0xFF has y >= p with the sign bit set (non-canonical).
    byte[] allZero = new byte[NodeId.PUBLIC_KEYLEN];
    byte[] allFf = new byte[NodeId.PUBLIC_KEYLEN];
    Arrays.fill(allFf, (byte) 0xFF);

    for (byte[] malformedKey : new byte[][] {allZero, allFf}) {
      withHandshakeAwaitingPublicKey(
          (connectionHandler, peerInHandshake, peerIdentity, accepted) -> {
            ByteBuffer buffer = ByteBuffer.allocate(1 + NodeId.PUBLIC_KEYLEN);
            buffer.put(Command.SEND_PUBLIC_KEY);
            buffer.put(malformedKey);
            buffer.flip();

            // Invokes the parser directly: without the fix the IllegalArgumentException from
            // NodeId.importPublic escapes here (unwrapped by invokeParse), with the fix the
            // rejection is handled like the KademliaId mismatch.
            boolean keepProcessing = invokeParse(connectionHandler, peerInHandshake, buffer);

            assertFalse(keepProcessing, "the read event must not be processed further");
            assertEquals(2, peerInHandshake.getStatus(), "disconnect status must be set");
            assertFalse(
                accepted.isOpen(), "the socket must be closed exactly like the mismatch path");
          });
    }
  }

  /**
   * A malformed key that arrives split across two reads goes through the REDPANDAJ-2FA carry-over
   * (stash + prepend) before it reaches the import — the reassembled key must hit the exact same
   * quiet rejection as a contiguous one.
   */
  @Test
  void malformedPublicKeySplitAcrossTwoReadsIsStillRejectedQuietly() throws Exception {
    withHandshakeAwaitingPublicKey(
        (connectionHandler, peerInHandshake, peerIdentity, accepted) -> {
          byte[] malformedKey = new byte[NodeId.PUBLIC_KEYLEN]; // all-zero, see test above
          int firstChunk = 40;

          ByteBuffer head = ByteBuffer.allocate(1 + firstChunk);
          head.put(Command.SEND_PUBLIC_KEY);
          head.put(malformedKey, 0, firstChunk);
          head.flip();

          assertTrue(
              invokeParse(connectionHandler, peerInHandshake, head),
              "incomplete command must wait for more bytes");
          assertEquals(
              1 + firstChunk,
              peerInHandshake.plaintextHandshakeCarryLength(),
              "the incomplete command must be stashed");
          assertTrue(accepted.isOpen(), "the connection must stay open while waiting");

          ByteBuffer tail = ByteBuffer.allocate(NodeId.PUBLIC_KEYLEN - firstChunk);
          tail.put(malformedKey, firstChunk, NodeId.PUBLIC_KEYLEN - firstChunk);
          tail.flip();
          ByteBuffer reassembled = peerInHandshake.prependPlaintextHandshakeCarry(tail);

          boolean keepProcessing = invokeParse(connectionHandler, peerInHandshake, reassembled);

          assertFalse(keepProcessing, "the read event must not be processed further");
          assertEquals(2, peerInHandshake.getStatus(), "disconnect status must be set");
          assertFalse(accepted.isOpen(), "the socket must be closed");
        });
  }

  /** Interop guard: a conforming peer's public key still completes this handshake step. */
  @Test
  void validPublicKeyStillCompletesTheHandshakeStep() throws Exception {
    withHandshakeAwaitingPublicKey(
        (connectionHandler, peerInHandshake, peerIdentity, accepted) -> {
          ByteBuffer buffer = ByteBuffer.allocate(1 + NodeId.PUBLIC_KEYLEN);
          buffer.put(Command.SEND_PUBLIC_KEY);
          buffer.put(peerIdentity.exportPublic());
          buffer.flip();

          boolean keepProcessing = invokeParse(connectionHandler, peerInHandshake, buffer);

          assertTrue(keepProcessing, "a valid key must not tear down the connection");
          assertEquals(
              -1, peerInHandshake.getStatus(), "status must advance to awaiting encryption");
          assertNotNull(peerInHandshake.getNodeId());
          assertNotNull(peerInHandshake.getPeer().getNodeId());
          assertTrue(accepted.isOpen(), "the connection must stay open");
        });
  }

  /** The pre-existing semantic rejection must be unchanged by the malformed-key guard. */
  @Test
  void kademliaIdMismatchStillClosesTheConnection() throws Exception {
    withHandshakeAwaitingPublicKey(
        (connectionHandler, peerInHandshake, peerIdentity, accepted) -> {
          // A perfectly well-formed key pair — just not the one the peer identified itself with.
          byte[] otherExport = NodeId.generateWithSimpleKey().exportPublic();
          ByteBuffer buffer = ByteBuffer.allocate(1 + NodeId.PUBLIC_KEYLEN);
          buffer.put(Command.SEND_PUBLIC_KEY);
          buffer.put(otherExport);
          buffer.flip();

          boolean keepProcessing = invokeParse(connectionHandler, peerInHandshake, buffer);

          assertFalse(keepProcessing, "the read event must not be processed further");
          assertEquals(2, peerInHandshake.getStatus(), "disconnect status must be set");
          assertFalse(accepted.isOpen(), "the socket must be closed");
        });
  }

  private interface HandshakeScenario {
    void run(
        ConnectionHandler connectionHandler,
        PeerInHandshake peerInHandshake,
        NodeId peerIdentity,
        SocketChannel accepted)
        throws Exception;
  }

  /**
   * Sets up the state right after parseHandshake(): the peer's identity (KademliaId) is known from
   * the 30-byte handshake, we asked for its public key (status 1) and are waiting for it — over a
   * real accepted socket so closing is observable.
   */
  private static void withHandshakeAwaitingPublicKey(HandshakeScenario scenario) throws Exception {
    ByteBufferPool.init();
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    ConnectionHandler connectionHandler = new ConnectionHandler(serverContext, false);

    try (ServerSocketChannel serverSocketChannel = ServerSocketChannel.open()) {
      serverSocketChannel.configureBlocking(false);
      serverSocketChannel.bind(new InetSocketAddress("127.0.0.1", 0));
      int port = serverSocketChannel.socket().getLocalPort();

      try (SocketChannel client = SocketChannel.open(new InetSocketAddress("127.0.0.1", port))) {
        SocketChannel accepted;
        do {
          accepted = serverSocketChannel.accept();
        } while (accepted == null);
        accepted.configureBlocking(false);

        NodeId peerIdentity = NodeId.generateWithSimpleKey();
        PeerInHandshake peerInHandshake = new PeerInHandshake("127.0.0.1", accepted);
        peerInHandshake.setPeer(new Peer("127.0.0.1", 0));
        peerInHandshake.setProtocolVersion(23);
        peerInHandshake.setLightClient(true);
        peerInHandshake.setIdentity(peerIdentity.getKademliaId());
        peerInHandshake.setStatus(1);

        try {
          scenario.run(connectionHandler, peerInHandshake, peerIdentity, accepted);
        } finally {
          accepted.close();
        }
      }
    }
  }

  /**
   * Calls the private parser and unwraps reflection's InvocationTargetException, so a leaking
   * IllegalArgumentException fails the test as itself (the REDPANDAJ-2EH symptom), not as a
   * reflection wrapper.
   */
  private static boolean invokeParse(
      ConnectionHandler connectionHandler, PeerInHandshake peerInHandshake, ByteBuffer buffer)
      throws Exception {
    Method parse =
        ConnectionHandler.class.getDeclaredMethod(
            "parsePlaintextHandshakeCommands", PeerInHandshake.class, ByteBuffer.class);
    parse.setAccessible(true);
    try {
      return (boolean) parse.invoke(connectionHandler, peerInHandshake, buffer);
    } catch (java.lang.reflect.InvocationTargetException e) {
      if (e.getCause() instanceof Exception cause) {
        throw cause;
      }
      throw e;
    }
  }
}
