package im.redpanda.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import org.junit.Test;

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
public class ConnectionHandlerCoalescedPublicKeyTest {

  static {
    java.security.Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  @Test
  public void coalescedRequestAndSendPublicKeyArePromotedInOneEvent() throws Exception {
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
              "expected the accepted channel to become readable", selector.select(10_000) > 0);
          SelectionKey readyKey = selector.selectedKeys().iterator().next();

          Method handlePeerInHandshake =
              ConnectionHandler.class.getDeclaredMethod(
                  "handlePeerInHandshake", SelectionKey.class);
          handlePeerInHandshake.setAccessible(true);
          handlePeerInHandshake.invoke(connectionHandler, readyKey);

          // Without the fix only REQUEST_PUBLIC_KEY was consumed and the status stayed 1, so the
          // handshake could never reach the encryption step.
          assertEquals(
              "the coalesced SEND_PUBLIC_KEY must have been parsed in the same read event",
              -1,
              peerInHandshake.getStatus());
          assertNotNull("the peer's NodeId must be known now", peerInHandshake.getNodeId());
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
              "expected SEND_PUBLIC_KEY followed by ACTIVATE_ENCRYPTION",
              expected,
              inbound.remaining());
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
