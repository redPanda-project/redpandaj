package im.redpanda.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import im.redpanda.core.Command;
import im.redpanda.core.ServerContext;
import im.redpanda.identity.NodeId;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import org.junit.jupiter.api.Test;

/**
 * REDPANDAJ-2FA regression: {@code ConnectionHandler.handlePeerInHandshake} must not drop the
 * peer's SEND_PUBLIC_KEY when it arrives coalesced with the peer's own REQUEST_PUBLIC_KEY in a
 * single read().
 *
 * <p>Both commands are written by the light client within one event-loop turn (it answers our
 * REQUEST_PUBLIC_KEY right after asking for ours), so the kernel regularly delivers them as one
 * segment. Parsing only the first command left the handshake in status 1 forever — we never sent
 * our ACTIVATE_ENCRYPTION, encryption never activated, and the client kept writing requests into a
 * connection the node would never answer.
 */
class ConnectionHandlerCoalescedPublicKeyTest {

  static {
    java.security.Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  /**
   * The other half of REDPANDAJ-2FA: a command whose payload has not fully arrived yet must be
   * carried over to the next read. SEND_PUBLIC_KEY used to throw BufferUnderflowException here and
   * ACTIVATE_ENCRYPTION closed the connection outright.
   */
  @Test
  void sendPublicKeySplitAcrossTwoReadsIsCarriedOver() throws Exception {
    ByteBufferPool.init();
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    ConnectionHandler connectionHandler = new ConnectionHandler(serverContext, false);

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

        NodeId peerIdentity = NodeId.generateWithSimpleKey();
        PeerInHandshake peerInHandshake = new PeerInHandshake("127.0.0.1", accepted);
        peerInHandshake.setPeer(new Peer("127.0.0.1", 0));
        peerInHandshake.setProtocolVersion(23);
        peerInHandshake.setLightClient(true);
        peerInHandshake.setIdentity(peerIdentity.getKademliaId());
        peerInHandshake.setStatus(1);

        SelectionKey key = accepted.register(selector, SelectionKey.OP_READ);
        key.attach(peerInHandshake);
        peerInHandshake.setKey(key);
        connectionHandler.addPeerInHandshake(peerInHandshake);

        Method handlePeerInHandshake =
            ConnectionHandler.class.getDeclaredMethod("handlePeerInHandshake", SelectionKey.class);
        handlePeerInHandshake.setAccessible(true);

        try {
          byte[] peerPublicExport = peerIdentity.exportPublic();
          int firstChunk = 30; // command byte + 29 of the 64 key bytes

          ByteBuffer head = ByteBuffer.allocate(1 + firstChunk);
          head.put(Command.SEND_PUBLIC_KEY);
          head.put(peerPublicExport, 0, firstChunk);
          head.flip();
          client.write(head);

          assertTrue(selector.select(10_000) > 0, "expected a first readable event");
          selector.selectedKeys().clear();
          handlePeerInHandshake.invoke(connectionHandler, key);

          assertEquals(
              1 + firstChunk,
              peerInHandshake.plaintextHandshakeCarryLength(),
              "the incomplete command must be stashed, not consumed or dropped");
          assertEquals(1, peerInHandshake.getStatus(), "still waiting for the key");
          assertTrue(accepted.isOpen(), "the connection must stay open");

          ByteBuffer tail = ByteBuffer.allocate(peerPublicExport.length - firstChunk);
          tail.put(peerPublicExport, firstChunk, peerPublicExport.length - firstChunk);
          tail.flip();
          client.write(tail);

          assertTrue(selector.select(10_000) > 0, "expected a second readable event");
          selector.selectedKeys().clear();
          handlePeerInHandshake.invoke(connectionHandler, key);

          assertEquals(
              -1,
              peerInHandshake.getStatus(),
              "the split SEND_PUBLIC_KEY must be decoded once the tail arrived");
          assertEquals(0, peerInHandshake.plaintextHandshakeCarryLength());
          assertNotNull(peerInHandshake.getNodeId());
        } finally {
          connectionHandler.removePeerInHandshake(peerInHandshake);
        }
      }
    }
  }

  @Test
  void coalescedRequestAndSendPublicKeyArePromotedInOneEvent() throws Exception {
    ByteBufferPool.init();
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    ConnectionHandler connectionHandler = new ConnectionHandler(serverContext, false);

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

        // State right after parseHandshake(): the peer's identity (KademliaId) is known from the
        // 30-byte handshake, we asked for its public key (status 1) and are waiting for it.
        NodeId peerIdentity = NodeId.generateWithSimpleKey();
        PeerInHandshake peerInHandshake = new PeerInHandshake("127.0.0.1", accepted);
        peerInHandshake.setPeer(new Peer("127.0.0.1", 0));
        peerInHandshake.setProtocolVersion(23);
        peerInHandshake.setLightClient(true);
        peerInHandshake.setIdentity(peerIdentity.getKademliaId());
        peerInHandshake.setStatus(1);

        SelectionKey key = accepted.register(selector, SelectionKey.OP_READ);
        key.attach(peerInHandshake);
        peerInHandshake.setKey(key);
        connectionHandler.addPeerInHandshake(peerInHandshake);

        try {
          // One write() => one segment => one read() server-side: the client asks for our public
          // key and answers ours in the same turn.
          byte[] peerPublicExport = peerIdentity.exportPublic();
          ByteBuffer combined = ByteBuffer.allocate(1 + 1 + peerPublicExport.length);
          combined.put(Command.REQUEST_PUBLIC_KEY);
          combined.put(Command.SEND_PUBLIC_KEY);
          combined.put(peerPublicExport);
          combined.flip();
          client.write(combined);

          assertTrue(
              selector.select(10_000) > 0, "expected the accepted channel to become readable");
          SelectionKey readyKey = selector.selectedKeys().iterator().next();

          Method handlePeerInHandshake =
              ConnectionHandler.class.getDeclaredMethod(
                  "handlePeerInHandshake", SelectionKey.class);
          handlePeerInHandshake.setAccessible(true);
          handlePeerInHandshake.invoke(connectionHandler, readyKey);

          // Without the fix only REQUEST_PUBLIC_KEY was consumed and the status stayed 1, so the
          // handshake could never reach the encryption step.
          assertEquals(
              -1,
              peerInHandshake.getStatus(),
              "the coalesced SEND_PUBLIC_KEY must have been parsed in the same read event");
          assertNotNull(peerInHandshake.getNodeId(), "the peer's NodeId must be known now");
          assertNotNull(peerInHandshake.getPeer().getNodeId());

          // Both replies must be on the wire: our public key (answer to REQUEST_PUBLIC_KEY) and
          // our ACTIVATE_ENCRYPTION, which the caller only sends once the status is -1.
          ByteBuffer inbound = ByteBuffer.allocate(256);
          int expected = 1 + NodeId.PUBLIC_KEYLEN + 1 + 32;
          long deadline = System.currentTimeMillis() + 5_000;
          // SocketChannel.open() hands back a BLOCKING channel, in which read() parks until bytes
          // arrive - a regression that writes fewer bytes than expected would hang the build
          // instead of failing it. Switch to non-blocking so the deadline below actually bounds
          // this loop (Copilot review, PR #288).
          client.configureBlocking(false);
          while (inbound.position() < expected && System.currentTimeMillis() < deadline) {
            if (client.read(inbound) == 0) {
              Thread.sleep(10);
            }
          }
          inbound.flip();
          assertEquals(
              expected,
              inbound.remaining(),
              "expected SEND_PUBLIC_KEY followed by ACTIVATE_ENCRYPTION");
          assertEquals(Command.SEND_PUBLIC_KEY, inbound.get());
          inbound.position(inbound.position() + NodeId.PUBLIC_KEYLEN);
          assertEquals(Command.ACTIVATE_ENCRYPTION, inbound.get());
        } finally {
          connectionHandler.removePeerInHandshake(peerInHandshake);
        }
      }
    }
  }
}
